using Microsoft.AspNetCore.SignalR;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Realtime;

public sealed class ProviderNotifier(IHubContext<ProviderHub> hub, TimeProvider time)
{
    public Task EventCreatedAsync(WebhookEvent webhookEvent, int orderId) =>
        hub.Clients.All.SendAsync("EventCreated", new EventCreatedMessage(
            webhookEvent.Id, time.GetUtcNow(), webhookEvent.Type, orderId));

    public Task AttemptStartedAsync(Delivery delivery, int attemptNumber, bool isDuplicateSend) =>
        hub.Clients.All.SendAsync("AttemptStarted", new AttemptStartedMessage(
            delivery.EventId, time.GetUtcNow(), delivery.Id, attemptNumber, delivery.IsReplay, isDuplicateSend));

    public Task AttemptCompletedAsync(Delivery delivery, int attemptNumber, int? statusCode, string? error, long durationMs, bool isDuplicateSend) =>
        hub.Clients.All.SendAsync("AttemptCompleted", new AttemptCompletedMessage(
            delivery.EventId, time.GetUtcNow(), delivery.Id, attemptNumber, statusCode, error, durationMs, isDuplicateSend));

    public Task DeliveryStatusChangedAsync(Delivery delivery) =>
        hub.Clients.All.SendAsync("DeliveryStatusChanged", new DeliveryStatusChangedMessage(
            delivery.EventId,
            time.GetUtcNow(),
            delivery.Id,
            delivery.Status,
            delivery.Status == DeliveryStatus.Pending ? delivery.NextAttemptAt : null));
}
