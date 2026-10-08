namespace WebhookLab.Provider.Data;

/// <summary>
/// One job of "get this event to the Receiver". The outbox row the dispatcher works from.
/// A replay creates a new Delivery for the same event (same event id), with IsReplay = true.
/// </summary>
public sealed class Delivery
{
    public int Id { get; init; }
    public required string EventId { get; init; }
    public DeliveryStatus Status { get; set; } = DeliveryStatus.Pending;

    /// <summary>Attempts made so far, including the one that succeeded.</summary>
    public int AttemptCount { get; set; }

    public DateTime NextAttemptAt { get; set; }
    public bool IsReplay { get; init; }
    public DateTime CreatedAt { get; init; }
    public List<DeliveryAttempt> Attempts { get; } = [];
}
