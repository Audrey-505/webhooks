namespace WebhookLab.Contracts;

/// <summary>The webhook event types the Provider sends.</summary>
public static class EventTypes
{
    public const string OrderCreated = "order.created";
    public const string PaymentSucceeded = "payment.succeeded";
    public const string PaymentRefunded = "payment.refunded";
}
