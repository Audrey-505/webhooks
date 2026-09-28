using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;
using WebhookLab.Receiver.Realtime;

namespace WebhookLab.Receiver.Processing;

/// <summary>
/// Applies stored events to the ledger. Each event's ledger change and its inbox status are
/// committed in one transaction, so a crash can never leave "charged but not marked processed".
/// </summary>
public sealed class InboxProcessor(
    ReceiverDbContext db,
    IEnumerable<IWebhookEventHandler> handlers,
    FaultSettings faults,
    TimeProvider time,
    ReceiverNotifier notifier,
    ILogger<InboxProcessor> logger)
{
    private readonly Dictionary<string, IWebhookEventHandler> _handlers = handlers.ToDictionary(h => h.EventType);

    /// <summary>Processes every due event, oldest first. Returns how many were attempted.</summary>
    public async Task<int> ProcessDueAsync(CancellationToken ct = default)
    {
        var now = time.GetUtcNow().UtcDateTime;
        var dueEventIds = await db.InboxEvents
            .Where(e => (e.Status == InboxStatus.Received || e.Status == InboxStatus.Deferred) && e.NextAttemptAt <= now)
            .OrderBy(e => e.ReceivedAt)
            .Select(e => e.EventId)
            .ToListAsync(ct);

        foreach (var eventId in dueEventIds)
            await ProcessOneAsync(eventId, ct);

        return dueEventIds.Count;
    }

    private async Task ProcessOneAsync(string eventId, CancellationToken ct)
    {
        db.ChangeTracker.Clear();
        // POST /api/reset can delete the row between the due-list query and this load; nothing left to do.
        var inboxEvent = await db.InboxEvents.SingleOrDefaultAsync(e => e.EventId == eventId, ct);
        if (inboxEvent is null)
            return;

        if (!_handlers.TryGetValue(inboxEvent.Type, out var handler))
        {
            inboxEvent.Status = InboxStatus.Failed;
            inboxEvent.LastError = $"No handler for event type '{inboxEvent.Type}'.";
            await db.SaveChangesAsync(ct);
            await notifier.ProcessingCompletedAsync(eventId, inboxEvent.Status, inboxEvent.LastError);
            return;
        }

        OrderEventData data;
        try
        {
            // Deserializing here too: a corrupted/unparsable payload is handled by the same catch below
            // (counts an attempt, eventually Failed) instead of throwing before the try and aborting
            // every remaining due event in this pass.
            data = JsonSerializer.Deserialize<WebhookEnvelope>(inboxEvent.Payload, WebhookJson.Options)!.Data;
            await using var transaction = await db.Database.BeginTransactionAsync(ct);

            var result = await handler.HandleAsync(data, db, ct);
            await db.SaveChangesAsync(ct);

            if (faults.TryConsumeCrash())
                throw new InvalidOperationException("Injected crash: the ledger was written but never committed.");

            RecordResult(inboxEvent, result);
            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Leaving the try block disposed the uncommitted transaction, which rolled it back.
            // A crashed Deferred event goes back to Received (not back to Deferred) on purpose: it's
            // still due, and re-deriving whatever made it Deferred is unnecessary complexity for a lab.
            db.ChangeTracker.Clear();

            // Poison message: an event that always throws (a handler bug, a persistent DB error, or a
            // repeated injected crash) must not retry forever. Count this attempt and escalate to Failed
            // once it hits the same cap deferred events face, instead of leaving it stuck at NextAttemptAt
            // in the past and reprocessed every pass forever.
            var attempts = inboxEvent.ProcessingAttempts + 1;
            var status = attempts >= Backoff.MaxAttempts ? InboxStatus.Failed : InboxStatus.Received;
            if (status == InboxStatus.Failed)
                logger.LogError(ex, "Processing {EventId} failed {Max} times; marked Failed", eventId, Backoff.MaxAttempts);
            else
                logger.LogWarning(ex, "Processing {EventId} failed (attempt {Attempt}/{Max}); retrying next pass", eventId, attempts, Backoff.MaxAttempts);

            await db.InboxEvents
                .Where(e => e.EventId == eventId)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(e => e.LastError, ex.Message)
                    .SetProperty(e => e.ProcessingAttempts, attempts)
                    .SetProperty(e => e.Status, status), ct);
            await notifier.ProcessingCompletedAsync(eventId, status, ex.Message);
            return;
        }

        await notifier.ProcessingCompletedAsync(eventId, inboxEvent.Status, inboxEvent.LastError);
        if (inboxEvent.Status == InboxStatus.Processed
            && await db.LedgerOrders.AsNoTracking().SingleOrDefaultAsync(o => o.OrderId == data.OrderId, ct) is { } order)
        {
            await notifier.LedgerChangedAsync(eventId, order);
        }
    }

    private void RecordResult(InboxEvent inboxEvent, HandlerResult result)
    {
        inboxEvent.ProcessingAttempts++;

        if (result == HandlerResult.Applied)
        {
            inboxEvent.Status = InboxStatus.Processed;
            inboxEvent.LastError = null;
        }
        else if (inboxEvent.ProcessingAttempts >= Backoff.MaxAttempts)
        {
            inboxEvent.Status = InboxStatus.Failed;
            inboxEvent.LastError = $"Still waiting on an earlier event after {Backoff.MaxAttempts} attempts.";
        }
        else
        {
            inboxEvent.Status = InboxStatus.Deferred;
            inboxEvent.NextAttemptAt = time.GetUtcNow().UtcDateTime + Backoff.Delay(inboxEvent.ProcessingAttempts, Random.Shared);
            inboxEvent.LastError = "Waiting on an earlier event (arrived out of order).";
        }
    }
}
