using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class OrderEndpointsTests
{
    [Fact]
    public async Task Create_saves_the_order_and_queues_an_order_created_webhook()
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsJsonAsync("/api/orders", new CreateOrderRequest(50m));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var created = (await response.Content.ReadFromJsonAsync<OrderActionResponse>(ProviderFactory.Json))!;
        Assert.Equal(OrderStatus.Created, created.Status);

        var webhookEvent = await lab.WithDbAsync(db => db.Events.SingleAsync(e => e.Id == created.EventId));
        Assert.Equal(EventTypes.OrderCreated, webhookEvent.Type);
        var envelope = JsonSerializer.Deserialize<WebhookEnvelope>(webhookEvent.Payload, WebhookJson.Options)!;
        Assert.Equal(created.EventId, envelope.Id);
        Assert.Equal(new OrderEventData(created.OrderId, 50m), envelope.Data);

        var delivery = await lab.WithDbAsync(db => db.Deliveries.SingleAsync(d => d.EventId == created.EventId));
        Assert.Equal(DeliveryStatus.Pending, delivery.Status);
        Assert.Equal(0, delivery.AttemptCount);
        Assert.False(delivery.IsReplay);
        Assert.Equal(lab.Time.GetUtcNow().UtcDateTime, delivery.NextAttemptAt);
    }

    [Fact]
    public async Task Pay_marks_the_order_paid_and_queues_payment_succeeded()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();

        var paid = await lab.PayAsync(order.OrderId);

        Assert.Equal(OrderStatus.Paid, paid.Status);
        Assert.NotEqual(order.EventId, paid.EventId);
        Assert.Equal(EventTypes.PaymentSucceeded, (await lab.WithDbAsync(db => db.Events.SingleAsync(e => e.Id == paid.EventId))).Type);
        Assert.Equal(1, await lab.WithDbAsync(db => db.Deliveries.CountAsync(d => d.EventId == paid.EventId)));
    }

    [Fact]
    public async Task Refund_after_payment_queues_payment_refunded()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.PayAsync(order.OrderId);

        var response = await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/refund", null);

        response.EnsureSuccessStatusCode();
        var refunded = (await response.Content.ReadFromJsonAsync<OrderActionResponse>(ProviderFactory.Json))!;
        Assert.Equal(OrderStatus.Refunded, refunded.Status);
        Assert.Equal(EventTypes.PaymentRefunded, (await lab.WithDbAsync(db => db.Events.SingleAsync(e => e.Id == refunded.EventId))).Type);
    }

    [Fact]
    public async Task Paying_twice_is_a_conflict_and_queues_nothing()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.PayAsync(order.OrderId);

        var second = await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/pay", null);

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Equal(2, await lab.WithDbAsync(db => db.Events.CountAsync()));
        Assert.Equal(2, await lab.WithDbAsync(db => db.Deliveries.CountAsync()));
    }

    [Fact]
    public async Task Refunding_an_unpaid_order_is_a_conflict()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();

        var response = await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/refund", null);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal(OrderStatus.Created, (await lab.WithDbAsync(db => db.Orders.SingleAsync())).Status);
    }

    [Fact]
    public async Task Unknown_order_is_not_found()
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsync("/api/orders/999/pay", null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public async Task Non_positive_amount_is_rejected(int amount)
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsJsonAsync("/api/orders", new CreateOrderRequest(amount));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, await lab.WithDbAsync(db => db.Orders.CountAsync()));
    }
}
