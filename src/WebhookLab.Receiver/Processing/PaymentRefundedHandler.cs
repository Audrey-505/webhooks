using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public sealed class PaymentRefundedHandler : IWebhookEventHandler
{
    public string EventType => EventTypes.PaymentRefunded;

    public async Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct)
    {
        var order = await db.LedgerOrders.FindAsync([data.OrderId], ct);
        if (order is not { Status: LedgerStatus.Paid })
            return HandlerResult.Deferred;

        order.Status = LedgerStatus.Refunded;
        order.RefundCount++;
        return HandlerResult.Applied;
    }
}
