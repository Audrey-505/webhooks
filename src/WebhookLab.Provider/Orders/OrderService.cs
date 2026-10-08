using System.Text.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Realtime;

namespace WebhookLab.Provider.Orders;

public enum OrderActionStatus
{
    Done,
    NotFound,
    /// <summary>The order isn't in the state this action needs (e.g. refunding an unpaid order).</summary>
    InvalidState,
}

public sealed record OrderActionResult(OrderActionStatus Status, Order? Order = null, string? EventId = null);

/// <summary>
/// Changes an order and, in the SAME transaction, records the webhook event plus a Pending delivery
/// (the outbox pattern). All three are saved or none are: an order change can't happen without its
/// webhook, and no webhook can describe a change that didn't happen. Sending is the dispatcher's job.
/// </summary>
public sealed class OrderService(ProviderDbContext db, TimeProvider time, ProviderNotifier notifier)
{
    public async Task<OrderActionResult> CreateAsync(decimal amount)
    {
        await using var transaction = await db.Database.BeginTransactionAsync();
        var order = new Order { Amount = amount };
        db.Orders.Add(order);
        await db.SaveChangesAsync(); // assigns order.Id, which the event payload needs

        var (webhookEvent, delivery) = AddEventWithDelivery(EventTypes.OrderCreated, order);
        await db.SaveChangesAsync();
        await transaction.CommitAsync();

        await notifier.EventCreatedAsync(webhookEvent, order.Id);
        await notifier.DeliveryStatusChangedAsync(delivery);
        return new OrderActionResult(OrderActionStatus.Done, order, webhookEvent.Id);
    }

    public Task<OrderActionResult> PayAsync(int orderId) =>
        TransitionAsync(orderId, from: OrderStatus.Created, to: OrderStatus.Paid, EventTypes.PaymentSucceeded);

    public Task<OrderActionResult> RefundAsync(int orderId) =>
        TransitionAsync(orderId, from: OrderStatus.Paid, to: OrderStatus.Refunded, EventTypes.PaymentRefunded);

    private async Task<OrderActionResult> TransitionAsync(int orderId, OrderStatus from, OrderStatus to, string eventType)
    {
        // Returning early without CommitAsync disposes the transaction, which rolls it back: nothing changes.
        await using var transaction = await db.Database.BeginTransactionAsync();
        var order = await db.Orders.FindAsync(orderId);
        if (order is null)
            return new OrderActionResult(OrderActionStatus.NotFound);
        if (order.Status != from)
            return new OrderActionResult(OrderActionStatus.InvalidState, order);

        order.Status = to;
        var (webhookEvent, delivery) = AddEventWithDelivery(eventType, order);
        await db.SaveChangesAsync();
        await transaction.CommitAsync();

        await notifier.EventCreatedAsync(webhookEvent, order.Id);
        await notifier.DeliveryStatusChangedAsync(delivery);
        return new OrderActionResult(OrderActionStatus.Done, order, webhookEvent.Id);
    }

    private (WebhookEvent Event, Delivery Delivery) AddEventWithDelivery(string type, Order order)
    {
        var now = time.GetUtcNow();
        var envelope = new WebhookEnvelope($"evt_{Guid.NewGuid():N}", type, now, new OrderEventData(order.Id, order.Amount));
        var webhookEvent = new WebhookEvent
        {
            Id = envelope.Id,
            Type = type,
            Payload = JsonSerializer.Serialize(envelope, WebhookJson.Options),
            CreatedAt = now.UtcDateTime,
        };
        db.Events.Add(webhookEvent);
        var delivery = new Delivery { EventId = webhookEvent.Id, NextAttemptAt = now.UtcDateTime, CreatedAt = now.UtcDateTime };
        db.Deliveries.Add(delivery);
        return (webhookEvent, delivery);
    }
}
