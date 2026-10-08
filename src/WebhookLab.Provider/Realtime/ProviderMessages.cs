using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Realtime;

// Every message carries EventId + At so the dashboard can merge both services into one timeline.
public sealed record EventCreatedMessage(string EventId, DateTimeOffset At, string Type, int OrderId);

public sealed record AttemptStartedMessage(
    string EventId, DateTimeOffset At, int DeliveryId, int AttemptNumber, bool IsReplay, bool IsDuplicateSend);

public sealed record AttemptCompletedMessage(
    string EventId, DateTimeOffset At, int DeliveryId, int AttemptNumber, int? StatusCode, string? Error, long DurationMs);

public sealed record DeliveryStatusChangedMessage(
    string EventId, DateTimeOffset At, int DeliveryId, DeliveryStatus Status, DateTime? NextAttemptAt);
