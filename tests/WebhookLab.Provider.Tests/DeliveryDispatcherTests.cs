using System.Net;
using System.Text.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class DeliveryDispatcherTests
{
    [Fact]
    public async Task Due_delivery_is_sent_signed_and_marked_succeeded()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();

        Assert.Equal(1, await lab.DispatchAsync());

        var received = Assert.Single(lab.Receiver.Received);
        Assert.Equal(order.EventId, received.Id);
        Assert.True(received.IsSignedWith(ProviderFactory.Secret, lab.Time.GetUtcNow()));
        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        Assert.Equal(1, delivery.AttemptCount);
        Assert.Equal(200, Assert.Single(delivery.Attempts).StatusCode);
    }

    [Fact]
    public async Task Failed_delivery_is_retried_after_its_backoff_not_before()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(HttpStatusCode.InternalServerError);
        var order = await lab.CreateOrderAsync();
        var sentAt = lab.Time.GetUtcNow().UtcDateTime;

        await lab.DispatchAsync();

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Pending, delivery.Status);
        Assert.Equal(1, delivery.AttemptCount);
        Assert.InRange(delivery.NextAttemptAt, sentAt.AddSeconds(2), sentAt.AddSeconds(2.5));

        lab.Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(0, await lab.DispatchAsync());

        lab.Time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(1, await lab.DispatchAsync());
        Assert.Equal(DeliveryStatus.Succeeded, Assert.Single(await lab.GetDeliveriesAsync(order.EventId)).Status);
        Assert.Equal(2, lab.Receiver.Received.Count);
    }

    [Fact]
    public async Task Timeouts_refused_connections_and_client_errors_are_all_retried()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.TimeOutNext();
        lab.Receiver.RefuseConnectionNext();
        lab.Receiver.RespondWith(HttpStatusCode.Unauthorized);
        var order = await lab.CreateOrderAsync();

        for (var pass = 0; pass < 4; pass++)
        {
            await lab.DispatchAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20));
        }

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        Assert.Equal(new int?[] { null, null, 401, 200 }, delivery.Attempts.Select(a => a.StatusCode));
        Assert.NotNull(delivery.Attempts[0].Error);
        Assert.NotNull(delivery.Attempts[1].Error);
    }

    [Fact]
    public async Task Delivery_is_dead_after_max_attempts_and_never_sent_again()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(Enumerable.Repeat(HttpStatusCode.InternalServerError, Backoff.MaxAttempts + 1).ToArray());
        var order = await lab.CreateOrderAsync();

        for (var pass = 0; pass < Backoff.MaxAttempts + 2; pass++)
        {
            await lab.DispatchAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20));
        }

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Dead, delivery.Status);
        Assert.Equal(Backoff.MaxAttempts, delivery.AttemptCount);
        Assert.Equal(Backoff.MaxAttempts, lab.Receiver.Received.Count);
    }

    [Fact]
    public async Task Oldest_delivery_is_sent_first()
    {
        await using var lab = new ProviderFactory();
        var first = await lab.CreateOrderAsync();
        var second = await lab.CreateOrderAsync();

        await lab.DispatchAsync();

        Assert.Equal([first.EventId, second.EventId], lab.Receiver.Received.Select(r => r.Id));
    }

    [Fact]
    public async Task Every_attempt_resends_the_same_body_with_a_fresh_signature()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(HttpStatusCode.ServiceUnavailable);
        var order = await lab.CreateOrderAsync();

        await lab.DispatchAsync();
        lab.Time.Advance(TimeSpan.FromSeconds(10));
        await lab.DispatchAsync();

        var (attempt1, attempt2) = (lab.Receiver.Received[0], lab.Receiver.Received[1]);
        Assert.Equal(attempt1.Body, attempt2.Body);
        Assert.Equal(attempt1.Id, attempt2.Id);
        Assert.NotEqual(attempt1.Timestamp, attempt2.Timestamp);
        Assert.Equal(order.EventId, JsonSerializer.Deserialize<WebhookEnvelope>(attempt2.Body, WebhookJson.Options)!.Id);
    }
}
