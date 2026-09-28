namespace WebhookLab.Receiver.Data;

/// <summary>
/// The store's view of an order. ChargeCount/RefundCount exist so you can *see* whether
/// a payment was applied once (correct) or twice (the bug idempotency prevents).
/// </summary>
public sealed class LedgerOrder
{
    public int OrderId { get; init; }
    public decimal Amount { get; init; }
    public LedgerStatus Status { get; set; } = LedgerStatus.Created;
    public int ChargeCount { get; set; }
    public int RefundCount { get; set; }
}
