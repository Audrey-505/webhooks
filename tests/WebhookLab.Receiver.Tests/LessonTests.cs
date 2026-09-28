using System.Net;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

/// <summary>Each test is one of the lab's lessons, stated as the behavior it proves.</summary>
public class LessonTests
{
    private const int OrderId = 42;

    [Fact]
    public async Task Same_event_twice_charges_customer_once()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        var payment = TestEvents.Paid(OrderId);

        await lab.SendAndProcessAsync(payment);
        await lab.SendAndProcessAsync(payment);

        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Lost_200_then_retry_is_skipped_as_duplicate()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 1, CrashNext: 0, DedupeOff: false));
        var payment = TestEvents.Paid(OrderId);

        // Attempt 1: the Receiver stores the event, but answers after the sender has given up.
        using var senderTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lab.SendAsync(payment, ct: senderTimeout.Token));
        await lab.WaitUntilStoredAsync(payment.Id);

        // Attempt 2: the sender never heard back, so it retries the same event id.
        var retry = await lab.SendAsync(payment);
        await lab.ProcessInboxAsync();

        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Refund_before_payment_is_deferred_then_applied()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        var payment = TestEvents.Paid(OrderId);
        var refund = TestEvents.Refunded(OrderId);

        await lab.SendAsync(refund); // arrives first
        lab.Time.Advance(TimeSpan.FromSeconds(1));
        await lab.SendAsync(payment);
        await lab.ProcessInboxAsync();

        Assert.Equal(InboxStatus.Deferred, (await lab.GetInboxEventAsync(refund.Id))!.Status);
        Assert.Equal(LedgerStatus.Paid, (await lab.GetLedgerOrderAsync(OrderId))!.Status);

        lab.Time.Advance(TimeSpan.FromSeconds(3));
        await lab.ProcessInboxAsync();

        var order = await lab.GetLedgerOrderAsync(OrderId);
        Assert.Equal(LedgerStatus.Refunded, order!.Status);
        Assert.Equal(1, order.RefundCount);
    }

    [Fact]
    public async Task Crash_mid_processing_is_retried_without_double_charge()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 1, DedupeOff: false));

        await lab.SendAndProcessAsync(TestEvents.Paid(OrderId)); // crashes, rolls back
        await lab.ProcessInboxAsync();                           // retried

        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Forged_signature_is_rejected()
    {
        await using var lab = new ReceiverFactory();
        var forged = TestEvents.Paid(OrderId);

        var response = await lab.SendAsync(forged, secret: "not-the-real-secret");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync(forged.Id));
    }

    [Fact]
    public async Task Dedupe_off_charges_customer_twice()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 0, DedupeOff: true));
        var payment = TestEvents.Paid(OrderId);

        await lab.SendAndProcessAsync(payment);
        await lab.SendAndProcessAsync(payment);

        // The bug idempotency prevents, made visible.
        Assert.Equal(2, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }
}
