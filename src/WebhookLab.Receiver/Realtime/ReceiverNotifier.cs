using Microsoft.AspNetCore.SignalR;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Realtime;

public sealed class ReceiverNotifier(IHubContext<ReceiverHub> hub, TimeProvider time)
{
    public Task WebhookReceivedAsync(string eventId, WebhookOutcome outcome) =>
        hub.Clients.All.SendAsync("WebhookReceived", new WebhookReceivedMessage(eventId, time.GetUtcNow(), outcome));

    public Task ProcessingCompletedAsync(string eventId, InboxStatus status, string? error) =>
        hub.Clients.All.SendAsync("ProcessingCompleted", new ProcessingCompletedMessage(eventId, time.GetUtcNow(), status, error));

    public Task LedgerChangedAsync(string eventId, LedgerOrder order) =>
        hub.Clients.All.SendAsync("LedgerChanged", new LedgerChangedMessage(
            eventId, time.GetUtcNow(), order.OrderId, order.ChargeCount, order.RefundCount, order.Status));
}
