using System.Diagnostics;
using System.Net;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

public class WebhookEndpointTests
{
    [Fact]
    public async Task Valid_webhook_is_stored_as_received_and_acknowledged()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);

        var response = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var stored = await lab.GetInboxEventAsync(created.Id);
        Assert.NotNull(stored);
        Assert.Equal(InboxStatus.Received, stored.Status);
        Assert.Equal(created.Type, stored.Type);
    }

    [Fact]
    public async Task Duplicate_webhook_is_acknowledged_and_stored_once()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);
        await lab.SendAsync(created);

        var second = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Equal(1, await lab.WithDbAsync(db => db.InboxEvents.CountAsync()));
    }

    [Fact]
    public async Task Wrong_secret_is_rejected_and_nothing_is_stored()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);

        var response = await lab.SendAsync(created, secret: "attacker-guess");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync(created.Id));
    }

    [Fact]
    public async Task Old_timestamp_is_rejected()
    {
        await using var lab = new ReceiverFactory();

        var response = await lab.SendAsync(TestEvents.Created(42), signedAt: lab.Time.GetUtcNow().AddMinutes(-10));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Body_id_that_does_not_match_the_header_is_a_bad_request()
    {
        await using var lab = new ReceiverFactory();

        var response = await lab.SendAsync(TestEvents.Created(42), headerId: "evt_different");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task FailNext_fault_returns_500_without_storing()
    {
        await using var lab = new ReceiverFactory();
        lab.Faults.Apply(new FaultSnapshot(FailNext: 1, SlowNext: 0, CrashNext: 0, DedupeOff: false));
        var created = TestEvents.Created(42);

        var response = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync(created.Id));
    }

    [Fact]
    public async Task SlowNext_fault_stores_the_event_then_answers_late()
    {
        await using var lab = new ReceiverFactory();
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 1, CrashNext: 0, DedupeOff: false));
        var created = TestEvents.Created(42);

        var stopwatch = Stopwatch.StartNew();
        var response = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(stopwatch.Elapsed >= lab.Faults.SlowResponseDelay);
        Assert.NotNull(await lab.GetInboxEventAsync(created.Id));
    }

    [Fact]
    public async Task DedupeOff_puts_a_duplicate_back_in_the_queue()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);
        await lab.SendAsync(created);
        await lab.WithDbAsync(db => db.InboxEvents
            .Where(e => e.EventId == created.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Status, InboxStatus.Processed)));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 0, DedupeOff: true));

        await lab.SendAsync(created);

        Assert.Equal(InboxStatus.Received, (await lab.GetInboxEventAsync(created.Id))!.Status);
    }
}
