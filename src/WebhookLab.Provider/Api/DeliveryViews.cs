using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Api;

/// <summary>What GET /api/deliveries returns: a delivery, its event type, and every attempt in order.</summary>
public sealed record DeliveryView(
    int Id,
    string EventId,
    string EventType,
    DeliveryStatus Status,
    int AttemptCount,
    /// <summary>Only meaningful while Pending — null otherwise, matching the hub's DeliveryStatusChanged.</summary>
    DateTime? NextAttemptAt,
    bool IsReplay,
    DateTime CreatedAt,
    /// <summary>True while ReorderNext chaos is holding this delivery back (see ReorderBuffer).</summary>
    bool IsHeld,
    IReadOnlyList<AttemptView> Attempts)
{
    public static DeliveryView From(Delivery delivery, string eventType, ReorderBuffer reorder) => new(
        delivery.Id,
        delivery.EventId,
        eventType,
        delivery.Status,
        delivery.AttemptCount,
        delivery.Status == DeliveryStatus.Pending ? delivery.NextAttemptAt : null,
        delivery.IsReplay,
        delivery.CreatedAt,
        reorder.IsHeld(delivery.Id),
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
