using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Realtime;

public enum WebhookOutcome
{
    Stored,
    Duplicate,
    DuplicateReprocessed,
    Rejected,
    FaultInjected,
}

// Every message carries EventId + At so the dashboard can merge both services into one timeline.
public sealed record WebhookReceivedMessage(string EventId, DateTimeOffset At, WebhookOutcome Outcome);

public sealed record ProcessingCompletedMessage(string EventId, DateTimeOffset At, InboxStatus Status, string? Error);

public sealed record LedgerChangedMessage(
    string EventId, DateTimeOffset At, int OrderId, int ChargeCount, int RefundCount, LedgerStatus Status);
