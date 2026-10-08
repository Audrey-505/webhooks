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
    TimeProvider time,
    ProviderNotifier notifier)
{
    /// <summary>Sends every due delivery, oldest first. Returns how many were sent.</summary>
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

            await DeliverAsync(delivery.Id, ct);
            sent++;
        }

        // ...and once the last one is collected (or reorder was switched off), send them all newest-first.
        if (!chaos.ReorderPending && reorder.HasHeld)
        {
            foreach (var deliveryId in reorder.ReleaseReversed())
            {
                await DeliverAsync(deliveryId, ct);
                sent++;
            }
        }

        return sent;
    }

    private async Task DeliverAsync(int deliveryId, CancellationToken ct)
    {
        db.ChangeTracker.Clear();
        // POST /api/reset can delete the row between the due-list query and this load; nothing left to do.
        var delivery = await db.Deliveries.SingleOrDefaultAsync(d => d.Id == deliveryId, ct);
        if (delivery is not { Status: DeliveryStatus.Pending })
            return;
        var webhookEvent = await db.Events.AsNoTracking().SingleAsync(e => e.Id == delivery.EventId, ct);

        if (chaos.Delay > TimeSpan.Zero)
            await Task.Delay(chaos.Delay, ct);

        // No database transaction is open here: never hold one across a network call that can take seconds.
        var attemptNumber = delivery.AttemptCount + 1;
        var result = await SendAndRecordAsync(delivery, webhookEvent, attemptNumber, isDuplicateSend: false, ct);
        if (chaos.TryConsumeDuplicate())
            await SendAndRecordAsync(delivery, webhookEvent, attemptNumber, isDuplicateSend: true, ct);

        RecordOutcome(delivery, result);
        await db.SaveChangesAsync(ct);
        await notifier.DeliveryStatusChangedAsync(delivery);
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
        await notifier.AttemptCompletedAsync(delivery, attemptNumber, result.StatusCode, result.Error, result.DurationMs);
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
