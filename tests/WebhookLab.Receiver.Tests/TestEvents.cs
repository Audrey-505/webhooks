using WebhookLab.Contracts;

namespace WebhookLab.Receiver.Tests;

public static class TestEvents
{
    public static WebhookEnvelope Created(int orderId) => New(EventTypes.OrderCreated, orderId);
    public static WebhookEnvelope Paid(int orderId) => New(EventTypes.PaymentSucceeded, orderId);
    public static WebhookEnvelope Refunded(int orderId) => New(EventTypes.PaymentRefunded, orderId);

    public static WebhookEnvelope New(string type, int orderId) =>
        new($"evt_{Guid.NewGuid():N}", type, DateTimeOffset.UnixEpoch, new OrderEventData(orderId, 50m));
}
