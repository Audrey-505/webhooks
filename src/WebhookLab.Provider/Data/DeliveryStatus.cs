namespace WebhookLab.Provider.Data;

public enum DeliveryStatus
{
    /// <summary>Not delivered yet; the dispatcher sends it once NextAttemptAt has passed.</summary>
    Pending,
    /// <summary>The Receiver answered 2xx.</summary>
    Succeeded,
    /// <summary>Gave up after Backoff.MaxAttempts failures (a "dead letter"). Can still be replayed.</summary>
    Dead,
}
