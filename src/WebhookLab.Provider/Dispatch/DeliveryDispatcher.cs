using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Realtime;

namespace WebhookLab.Provider.Dispatch;

/// <summary>
/// The outbox worker: finds deliveries that are due, sends them, records every attempt, and
/// reschedules failures with exponential backoff until the Receiver answers 2xx — or gives up
/// (Dead) after Backoff.MaxAttempts. Sending-side chaos is applied here.
/// </summary>
public sealed class DeliveryDispatcher(
    ProviderDbContext db,
    WebhookSender sender,
    ChaosSettings chaos,
    ReorderBuffer reorder,
    ResetSignal resetSignal,
    TimeProvider time,
    ProviderNotifier notifier)
{
    /// <summary>
    /// Sends every due delivery, oldest first; deliveries held back by ReorderNext are released
    /// newest-first once the last one is collected (or reorder is switched off). Returns how many
    /// sends actually happened.
    /// </summary>
    public async Task<int> DispatchDueAsync(CancellationToken ct = default)
    {
        var now = time.GetUtcNow().UtcDateTime;
        var due = await db.Deliveries
            .Where(d => d.Status == DeliveryStatus.Pending && d.NextAttemptAt <= now)
            .OrderBy(d => d.Id)
            .Select(d => new { d.Id, d.AttemptCount })
            .ToListAsync(ct);

        var sent = 0;
        foreach (var delivery in due)
        {
            if (reorder.IsHeld(delivery.Id))
                continue;

            // ReorderNext chaos: hold brand-new deliveries back instead of sending them now...
            if (delivery.AttemptCount == 0 && chaos.TryConsumeReorder())
            {
                reorder.Hold(delivery.Id);
                continue;
            }

            if (await DeliverAsync(delivery.Id, ct))
                sent++;
        }

        // ...and once the last one is collected (or reorder was switched off), send them all newest-first.
        if (!chaos.ReorderPending && reorder.HasHeld)
        {
            foreach (var deliveryId in reorder.ReleaseReversed())
            {
                if (await DeliverAsync(deliveryId, ct))
                    sent++;
            }
        }

        return sent;
    }

    /// <summary>Returns true only when a send actually happened (false for a skipped or reset-cancelled delivery).</summary>
    private async Task<bool> DeliverAsync(int deliveryId, CancellationToken ct)
    {
        // Captured once, up front: if POST /api/reset fires anywhere below, this token (linked into
        // every delay/send/load) cancels and we bail without saving or notifying further. It is NOT
        // full protection against every race (e.g. a concurrent non-reset delete), just this one.
        var resetToken = resetSignal.Token;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, resetToken);
        try
        {
            db.ChangeTracker.Clear();
            // POST /api/reset can delete the row between the due-list query and this load; nothing left to do.
            var delivery = await db.Deliveries.SingleOrDefaultAsync(d => d.Id == deliveryId, linked.Token);
            if (delivery is not { Status: DeliveryStatus.Pending })
                return false;
            var webhookEvent = await db.Events.AsNoTracking().SingleOrDefaultAsync(e => e.Id == delivery.EventId, linked.Token);
            if (webhookEvent is null)
                return false;

            if (chaos.Delay > TimeSpan.Zero)
                await Task.Delay(chaos.Delay, linked.Token);

            // No database transaction is open here: never hold one across a network call that can take seconds.
            var attemptNumber = delivery.AttemptCount + 1;
            var result = await SendAndRecordAsync(delivery, webhookEvent, attemptNumber, isDuplicateSend: false, linked.Token);
            if (chaos.TryConsumeDuplicate())
                await SendAndRecordAsync(delivery, webhookEvent, attemptNumber, isDuplicateSend: true, linked.Token);

            if (resetToken.IsCancellationRequested)
                return false; // reset fired right after the send: the row may already be gone, leave it alone

            RecordOutcome(delivery, result);
            await db.SaveChangesAsync(ct);
            await notifier.DeliveryStatusChangedAsync(delivery);
            return true;
        }
        catch (OperationCanceledException) when (resetToken.IsCancellationRequested)
        {
            // Reset fired mid-delay or mid-send. The row this delivery pointed at may already be
            // gone (or belong to a fresh lab run); don't save or push any more notifications for it.
            // Our own shutdown is a *different* cancellation (ct, not resetToken) and still propagates.
            return false;
        }
    }

    private async Task<SendResult> SendAndRecordAsync(
        Delivery delivery, WebhookEvent webhookEvent, int attemptNumber, bool isDuplicateSend, CancellationToken ct)
    {
        var startedAt = time.GetUtcNow().UtcDateTime;
        await notifier.AttemptStartedAsync(delivery, attemptNumber, isDuplicateSend);

        var result = await sender.SendAsync(webhookEvent, useWrongSecret: chaos.TryConsumeBadSignature(), ct);

        db.DeliveryAttempts.Add(new DeliveryAttempt
        {
            DeliveryId = delivery.Id,
            AttemptNumber = attemptNumber,
            IsDuplicateSend = isDuplicateSend,
            StartedAt = startedAt,
            DurationMs = result.DurationMs,
            StatusCode = result.StatusCode,
            Error = result.Error,
        });
        await notifier.AttemptCompletedAsync(delivery, attemptNumber, result.StatusCode, result.Error, result.DurationMs, isDuplicateSend);
        return result;
    }

    private void RecordOutcome(Delivery delivery, SendResult result)
    {
        delivery.AttemptCount++;

        if (result.IsSuccess)
            delivery.Status = DeliveryStatus.Succeeded;
        else if (delivery.AttemptCount >= Backoff.MaxAttempts)
            delivery.Status = DeliveryStatus.Dead; // a "dead letter": kept for inspection and replay
        else
            delivery.NextAttemptAt = time.GetUtcNow().UtcDateTime + Backoff.Delay(delivery.AttemptCount, Random.Shared);
    }
}
