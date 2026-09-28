namespace WebhookLab.Receiver.Data;

public enum InboxStatus
{
    /// <summary>Stored and acknowledged; waiting for the processor.</summary>
    Received,
    /// <summary>Applied to the ledger.</summary>
    Processed,
    /// <summary>Arrived before an event it depends on; will retry with backoff.</summary>
    Deferred,
    /// <summary>Gave up. Visible on the dashboard, never silently dropped.</summary>
    Failed,
}
