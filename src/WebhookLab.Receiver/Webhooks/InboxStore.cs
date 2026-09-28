using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Webhooks;

public enum StoreResult
{
    Stored,
    Duplicate,
    DuplicateReprocessed,
}

/// <summary>
/// Inserts an incoming event, letting the database detect duplicates. Insert-and-catch is used
/// instead of check-then-insert: two copies arriving at the same instant can't both pass.
/// </summary>
public sealed class InboxStore(ReceiverDbContext db, TimeProvider time)
{
    private const int SqliteConstraintPrimaryKey = 1555;
    private const int SqliteConstraintUnique = 2067;

    public async Task<StoreResult> StoreAsync(WebhookEnvelope envelope, string rawBody, bool reprocessDuplicates)
    {
        var now = time.GetUtcNow().UtcDateTime;
        db.InboxEvents.Add(new InboxEvent
        {
            EventId = envelope.Id,
            Type = envelope.Type,
            Payload = rawBody,
            ReceivedAt = now,
            NextAttemptAt = now,
        });

        // No cancellation token on purpose: once a genuine event arrives, keep it even if the sender hangs up.
        try
        {
            await db.SaveChangesAsync();
            return StoreResult.Stored;
        }
        catch (DbUpdateException ex) when (IsDuplicateKey(ex))
        {
            db.ChangeTracker.Clear();
        }

        if (!reprocessDuplicates)
            return StoreResult.Duplicate;

        // UNSAFE lab mode: queue the already-stored event again, so it gets applied twice. This
        // check-then-update can race with InboxProcessor picking the row up between the two, so a
        // requeue may occasionally be lost — acceptable here since the whole point is showing the bug.
        await db.InboxEvents
            .Where(e => e.EventId == envelope.Id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(e => e.Status, InboxStatus.Received)
                .SetProperty(e => e.NextAttemptAt, now));
        return StoreResult.DuplicateReprocessed;
    }

    private static bool IsDuplicateKey(DbUpdateException ex) =>
        ex.InnerException is SqliteException { SqliteExtendedErrorCode: SqliteConstraintPrimaryKey or SqliteConstraintUnique };
}
