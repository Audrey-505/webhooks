namespace WebhookLab.Provider.Data;

/// <summary>
/// The Provider's own record of an order — its source of truth. The Receiver only ever learns
/// about it through webhooks.
/// </summary>
public sealed class Order
{
    public int Id { get; init; }
    public decimal Amount { get; init; }
    public OrderStatus Status { get; set; } = OrderStatus.Created;
}
