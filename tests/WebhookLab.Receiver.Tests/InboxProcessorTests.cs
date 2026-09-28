using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

public class InboxProcessorTests
{
    private const int OrderId = 42;

    [Fact]
    public async Task Received_event_is_applied_and_marked_processed()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(OrderId);

        await lab.SendAndProcessAsync(created);

        Assert.Equal(InboxStatus.Processed, (await lab.GetInboxEventAsync(created.Id))!.Status);
        Assert.NotNull(await lab.GetLedgerOrderAsync(OrderId));
    }

    [Fact]
    public async Task Deferred_event_is_not_retried_before_its_backoff_elapses()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Paid(OrderId)); // no order yet → deferred, retry in ~2 s

        lab.Time.Advance(TimeSpan.FromSeconds(1));

        Assert.Equal(0, await lab.ProcessInboxAsync());
    }

    [Fact]
    public async Task Deferred_event_is_applied_once_its_dependency_arrives()
    {
        await using var lab = new ReceiverFactory();
        var payment = TestEvents.Paid(OrderId);
        await lab.SendAndProcessAsync(payment);
        Assert.Equal(InboxStatus.Deferred, (await lab.GetInboxEventAsync(payment.Id))!.Status);

        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Time.Advance(TimeSpan.FromSeconds(3));
        await lab.ProcessInboxAsync();

        Assert.Equal(InboxStatus.Processed, (await lab.GetInboxEventAsync(payment.Id))!.Status);
        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Deferred_event_fails_after_max_attempts()
    {
        await using var lab = new ReceiverFactory();
        var payment = TestEvents.Paid(OrderId); // its order never arrives
        await lab.SendAsync(payment);

        for (var attempt = 1; attempt <= Backoff.MaxAttempts; attempt++)
        {
            await lab.ProcessInboxAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20)); // longer than any backoff step
        }

        var stored = await lab.GetInboxEventAsync(payment.Id);
        Assert.Equal(InboxStatus.Failed, stored!.Status);
        Assert.Equal(Backoff.MaxAttempts, stored.ProcessingAttempts);
    }

    [Fact]
    public async Task Unknown_event_type_is_marked_failed()
    {
        await using var lab = new ReceiverFactory();
        var mystery = TestEvents.New("customer.teleported", OrderId);

        await lab.SendAndProcessAsync(mystery);

        var stored = await lab.GetInboxEventAsync(mystery.Id);
        Assert.Equal(InboxStatus.Failed, stored!.Status);
        Assert.Contains("customer.teleported", stored.LastError);
    }

    [Fact]
    public async Task Crash_rolls_back_the_ledger_change_and_leaves_the_event_queued()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 1, DedupeOff: false));
        var payment = TestEvents.Paid(OrderId);

        await lab.SendAndProcessAsync(payment);

        var stored = await lab.GetInboxEventAsync(payment.Id);
        Assert.Equal(InboxStatus.Received, stored!.Status);
        Assert.NotNull(stored.LastError);
        Assert.Equal(0, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Undeserializable_payload_counts_an_attempt_instead_of_aborting_the_pass()
    {
        await using var lab = new ReceiverFactory();
        var now = lab.Time.GetUtcNow().UtcDateTime;
        await lab.WithDbAsync(db =>
        {
            db.InboxEvents.Add(new InboxEvent
            {
                EventId = "evt_bad_payload",
                Type = EventTypes.PaymentSucceeded,
                Payload = "not json",
                ReceivedAt = now,
                NextAttemptAt = now,
            });
            return db.SaveChangesAsync();
        });

        var exception = await Record.ExceptionAsync(() => lab.ProcessInboxAsync());

        Assert.Null(exception);
        var stored = await lab.GetInboxEventAsync("evt_bad_payload");
        Assert.Equal(1, stored!.ProcessingAttempts);
        Assert.NotNull(stored.LastError);
    }

    [Fact]
    public async Task Event_that_keeps_crashing_fails_after_max_attempts()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: Backoff.MaxAttempts, DedupeOff: false));
        var payment = TestEvents.Paid(OrderId);
        await lab.SendAsync(payment);

        for (var attempt = 1; attempt <= Backoff.MaxAttempts; attempt++)
            await lab.ProcessInboxAsync();

        var stored = await lab.GetInboxEventAsync(payment.Id);
        Assert.Equal(InboxStatus.Failed, stored!.Status);
        Assert.Equal(Backoff.MaxAttempts, stored.ProcessingAttempts);
        Assert.Equal(0, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }
}
