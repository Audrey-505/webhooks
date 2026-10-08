using System.Net;
using System.Net.Http.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

/// <summary>The Provider-side lessons, each stated as the behavior it proves.</summary>
public class DeliveryLessonTests
{
    [Fact]
    public async Task Receiver_outage_is_retried_with_growing_backoff_then_delivered()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(HttpStatusCode.InternalServerError, HttpStatusCode.InternalServerError, HttpStatusCode.InternalServerError);
        var order = await lab.CreateOrderAsync();

        for (var pass = 0; pass < 4; pass++)
        {
            await lab.DispatchAsync();
            var pending = (await lab.GetDeliveriesAsync(order.EventId)).Single();
            if (pending.Status == DeliveryStatus.Pending)
                lab.Time.SetUtcNow(new DateTimeOffset(pending.NextAttemptAt, TimeSpan.Zero)); // jump exactly to the retry
        }

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        var gaps = delivery.Attempts.Zip(delivery.Attempts.Skip(1), (a, b) => (b.StartedAt - a.StartedAt).TotalSeconds).ToList();
        Assert.InRange(gaps[0], 2, 2.5);
        Assert.InRange(gaps[1], 4, 4.5);
        Assert.InRange(gaps[2], 8, 8.5);
    }

    [Fact]
    public async Task Dead_letter_is_delivered_by_replay()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(Enumerable.Repeat(HttpStatusCode.InternalServerError, Backoff.MaxAttempts).ToArray());
        var order = await lab.CreateOrderAsync();
        for (var pass = 0; pass < Backoff.MaxAttempts; pass++)
        {
            await lab.DispatchAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20));
        }
        Assert.Equal(DeliveryStatus.Dead, (await lab.GetDeliveriesAsync(order.EventId)).Single().Status);

        var response = await lab.CreateClient().PostAsync($"/api/events/{order.EventId}/replay", null);
        await lab.DispatchAsync();

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var deliveries = await lab.GetDeliveriesAsync(order.EventId);
        Assert.Equal([DeliveryStatus.Dead, DeliveryStatus.Succeeded], deliveries.Select(d => d.Status));
        Assert.True(deliveries[1].IsReplay);
    }

    [Fact]
    public async Task Replay_resends_the_same_event_id_so_the_receiver_can_spot_it()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.DispatchAsync();

        var response = await lab.CreateClient().PostAsync($"/api/events/{order.EventId}/replay", null);
        var replay = (await response.Content.ReadFromJsonAsync<ReplayResponse>(ProviderFactory.Json))!;
        await lab.DispatchAsync();

        Assert.Equal(order.EventId, replay.EventId);
        Assert.Equal(2, lab.Receiver.Received.Count);
        Assert.Equal(lab.Receiver.Received[0].Id, lab.Receiver.Received[1].Id);
        Assert.Equal(lab.Receiver.Received[0].Body, lab.Receiver.Received[1].Body);
    }

    [Fact]
    public async Task Replaying_an_unknown_event_is_not_found()
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsync("/api/events/evt_missing/replay", null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
