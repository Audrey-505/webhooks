using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Api;

/// <summary>What GET /api/deliveries returns: a delivery, its event type, and every attempt in order.</summary>
public sealed record DeliveryView(
    int Id,
    string EventId,
    string EventType,
    DeliveryStatus Status,
    int AttemptCount,
    DateTime NextAttemptAt,
    bool IsReplay,
    DateTime CreatedAt,
    IReadOnlyList<AttemptView> Attempts)
{
    public static DeliveryView From(Delivery delivery, string eventType) => new(
        delivery.Id,
        delivery.EventId,
        eventType,
        delivery.Status,
        delivery.AttemptCount,
        delivery.NextAttemptAt,
        delivery.IsReplay,
        delivery.CreatedAt,
        delivery.Attempts
            .OrderBy(a => a.Id)
            .Select(a => new AttemptView(a.AttemptNumber, a.IsDuplicateSend, a.StartedAt, a.DurationMs, a.StatusCode, a.Error))
            .ToList());
}

public sealed record AttemptView(
    int AttemptNumber,
    bool IsDuplicateSend,
    DateTime StartedAt,
    long DurationMs,
    int? StatusCode,
    string? Error);
