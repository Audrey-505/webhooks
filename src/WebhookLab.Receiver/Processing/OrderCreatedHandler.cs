using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public sealed class OrderCreatedHandler : IWebhookEventHandler
{
    public string EventType => EventTypes.OrderCreated;

    public async Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct)
    {
        if (await db.LedgerOrders.FindAsync([data.OrderId], ct) is null)
            db.LedgerOrders.Add(new LedgerOrder { OrderId = data.OrderId, Amount = data.Amount });

        return HandlerResult.Applied;
    }
}
