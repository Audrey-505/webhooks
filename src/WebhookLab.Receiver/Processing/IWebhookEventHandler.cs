using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public enum HandlerResult
{
    Applied,
    /// <summary>An event this one depends on hasn't been processed yet (out-of-order delivery).</summary>
    Deferred,
}

/// <summary>
/// Applies one event type to the ledger. Handlers only modify tracked entities; the caller saves,
/// so the ledger change and the inbox status commit in the same transaction.
/// </summary>
public interface IWebhookEventHandler
{
    string EventType { get; }

    Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct);
}
