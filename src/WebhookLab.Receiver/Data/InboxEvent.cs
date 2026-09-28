namespace WebhookLab.Receiver.Data;

/// <summary>One row per unique webhook event. EventId is the primary key, which is what makes dedupe safe.</summary>
public sealed class InboxEvent
{
    public required string EventId { get; init; }
    public required string Type { get; init; }
    public required string Payload { get; init; }
    public DateTime ReceivedAt { get; init; }
    public InboxStatus Status { get; set; } = InboxStatus.Received;
    public int ProcessingAttempts { get; set; }
    public DateTime NextAttemptAt { get; set; }
    public string? LastError { get; set; }
}
