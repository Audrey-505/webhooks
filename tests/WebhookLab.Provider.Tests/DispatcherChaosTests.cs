using System.Diagnostics;
using System.Net;
using System.Text.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class DispatcherChaosTests
{
    [Fact]
    public async Task DuplicateNext_sends_the_same_event_twice()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 1, DelaySeconds: 0, ReorderNext: 0, BadSignatureNext: 0));
        var order = await lab.CreateOrderAsync();

        await lab.DispatchAsync();

        Assert.Equal(2, lab.Receiver.Received.Count);
        Assert.All(lab.Receiver.Received, r => Assert.Equal(order.EventId, r.Id));
        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        Assert.Equal(1, delivery.AttemptCount);
        Assert.Equal([false, true], delivery.Attempts.Select(a => a.IsDuplicateSend));
    }

    [Fact]
    public async Task BadSignatureNext_signs_with_the_wrong_secret_once()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 0, BadSignatureNext: 1));
        lab.Receiver.RespondWith(HttpStatusCode.Unauthorized); // what the real Receiver answers to a forged signature
        await lab.CreateOrderAsync();

        await lab.DispatchAsync();
        lab.Time.Advance(TimeSpan.FromSeconds(10));
        await lab.DispatchAsync();

        Assert.False(lab.Receiver.Received[0].IsSignedWith(ProviderFactory.Secret, lab.Time.GetUtcNow()));
        Assert.True(lab.Receiver.Received[1].IsSignedWith(ProviderFactory.Secret, lab.Time.GetUtcNow()));
    }

    [Fact]
    public async Task ReorderNext_holds_deliveries_until_collected_then_sends_them_newest_first()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 2, BadSignatureNext: 0));
        var first = await lab.CreateOrderAsync();

        Assert.Equal(0, await lab.DispatchAsync());
        Assert.Empty(lab.Receiver.Received);

        var second = await lab.CreateOrderAsync();
        await lab.DispatchAsync();

        Assert.Equal([second.EventId, first.EventId], lab.Receiver.Received.Select(r => r.Id));
        Assert.False(lab.Reorder.HasHeld);
    }

    [Fact]
    public async Task Turning_reorder_off_releases_anything_still_held()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 3, BadSignatureNext: 0));
        var order = await lab.CreateOrderAsync();
        await lab.DispatchAsync();

        Assert.Empty(lab.Receiver.Received); // still held...
        Assert.True(lab.Reorder.HasHeld);

        lab.Chaos.Reset();
        await lab.DispatchAsync();

        Assert.Equal(order.EventId, Assert.Single(lab.Receiver.Received).Id); // ...then released
    }

    [Fact]
    public async Task DelaySeconds_waits_before_sending()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 1, ReorderNext: 0, BadSignatureNext: 0));
        await lab.CreateOrderAsync();

        var stopwatch = Stopwatch.StartNew();
        await lab.DispatchAsync();

        Assert.True(stopwatch.Elapsed >= TimeSpan.FromSeconds(1));
        Assert.Single(lab.Receiver.Received);
    }

    [Fact]
    public async Task Reset_during_the_chaos_delay_cancels_the_in_flight_send()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 1, ReorderNext: 0, BadSignatureNext: 0));
        await lab.CreateOrderAsync();

        var dispatch = lab.DispatchAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        var reset = await lab.CreateClient().PostAsync("/api/reset", null);

        await dispatch;

        reset.EnsureSuccessStatusCode();
        Assert.Empty(lab.Receiver.Received);
    }

    [Fact]
    public async Task Reordered_payment_and_refund_reach_the_receiver_refund_first()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.DispatchAsync();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 2, BadSignatureNext: 0));

        await lab.PayAsync(order.OrderId);
        await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/refund", null);
        await lab.DispatchAsync();

        var types = lab.Receiver.Received.Skip(1)
            .Select(r => JsonSerializer.Deserialize<WebhookEnvelope>(r.Body, WebhookJson.Options)!.Type);
        Assert.Equal([EventTypes.PaymentRefunded, EventTypes.PaymentSucceeded], types);
    }
}
