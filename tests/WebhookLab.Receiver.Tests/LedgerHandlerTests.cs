using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Processing;

namespace WebhookLab.Receiver.Tests;

public class LedgerHandlerTests
{
    private static readonly OrderEventData Order42 = new(OrderId: 42, Amount: 50m);

    [Fact]
    public async Task OrderCreated_adds_the_order_to_the_ledger()
    {
        using var test = new TestDb();

        var result = await new OrderCreatedHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        var order = await test.Context.LedgerOrders.FindAsync(42);
        Assert.Equal(LedgerStatus.Created, order!.Status);
        Assert.Equal(50m, order.Amount);
    }

    [Fact]
    public async Task OrderCreated_for_an_existing_order_changes_nothing()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m, Status = LedgerStatus.Paid, ChargeCount = 1 });
        await test.Context.SaveChangesAsync();

        var result = await new OrderCreatedHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        Assert.Equal(LedgerStatus.Paid, (await test.Context.LedgerOrders.FindAsync(42))!.Status);
    }

    [Fact]
    public async Task PaymentSucceeded_marks_the_order_paid_and_counts_the_charge()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m });
        await test.Context.SaveChangesAsync();

        var result = await new PaymentSucceededHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        var order = await test.Context.LedgerOrders.FindAsync(42);
        Assert.Equal(LedgerStatus.Paid, order!.Status);
        Assert.Equal(1, order.ChargeCount);
    }

    [Fact]
    public async Task PaymentSucceeded_before_the_order_exists_is_deferred()
    {
        using var test = new TestDb();

        var result = await new PaymentSucceededHandler().HandleAsync(Order42, test.Context, default);

        Assert.Equal(HandlerResult.Deferred, result);
    }

    [Fact]
    public async Task PaymentRefunded_marks_a_paid_order_refunded()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m, Status = LedgerStatus.Paid, ChargeCount = 1 });
        await test.Context.SaveChangesAsync();

        var result = await new PaymentRefundedHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        var order = await test.Context.LedgerOrders.FindAsync(42);
        Assert.Equal(LedgerStatus.Refunded, order!.Status);
        Assert.Equal(1, order.RefundCount);
    }

    [Fact]
    public async Task PaymentRefunded_before_the_payment_is_deferred()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m });
        await test.Context.SaveChangesAsync();

        var result = await new PaymentRefundedHandler().HandleAsync(Order42, test.Context, default);

        Assert.Equal(HandlerResult.Deferred, result);
    }
}
