namespace WebhookLab.Provider.Data;

/// <summary>One HTTP POST to the Receiver and what came back. The dashboard timeline is drawn from these.</summary>
public sealed class DeliveryAttempt
{
    public int Id { get; init; }
    public int DeliveryId { get; init; }
    public int AttemptNumber { get; init; }

    /// <summary>True for the extra copy sent by the DuplicateNext chaos setting.</summary>
    public bool IsDuplicateSend { get; init; }

    public DateTime StartedAt { get; init; }
    public long DurationMs { get; init; }

    /// <summary>Null when no response arrived (timeout, connection refused).</summary>
    public int? StatusCode { get; init; }

    public string? Error { get; init; }
}
