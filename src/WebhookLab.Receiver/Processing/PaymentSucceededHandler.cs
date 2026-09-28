using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public sealed class PaymentSucceededHandler : IWebhookEventHandler
{
    public string EventType => EventTypes.PaymentSucceeded;

    public async Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct)
    {
        var order = await db.LedgerOrders.FindAsync([data.OrderId], ct);
        if (order is null)
            return HandlerResult.Deferred;

        order.Status = LedgerStatus.Paid;
        // Deliberately not idempotent on its own: the inbox dedupe is what stops a double charge.
        order.ChargeCount++;
        return HandlerResult.Applied;
    }
}
