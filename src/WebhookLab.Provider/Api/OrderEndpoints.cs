using WebhookLab.Provider.Data;
using WebhookLab.Provider.Orders;

namespace WebhookLab.Provider.Api;

public sealed record CreateOrderRequest(decimal Amount);

public sealed record OrderActionResponse(int OrderId, OrderStatus Status, string EventId);

public static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/orders", async (CreateOrderRequest request, OrderService orders) =>
        {
            if (request.Amount <= 0)
                return Results.BadRequest("Amount must be greater than zero.");

            var result = await orders.CreateAsync(request.Amount);
            return Results.Created($"/api/orders/{result.Order!.Id}", ToResponse(result));
        });

        app.MapPost("/api/orders/{id:int}/pay", async (int id, OrderService orders) =>
            ToHttpResult(await orders.PayAsync(id)));

        app.MapPost("/api/orders/{id:int}/refund", async (int id, OrderService orders) =>
            ToHttpResult(await orders.RefundAsync(id)));

        return app;
    }

    private static IResult ToHttpResult(OrderActionResult result) => result.Status switch
    {
        OrderActionStatus.Done => Results.Ok(ToResponse(result)),
        OrderActionStatus.NotFound => Results.NotFound(),
        _ => Results.Conflict($"Order is {result.Order!.Status}; that action isn't allowed now."),
    };

    private static OrderActionResponse ToResponse(OrderActionResult result) =>
        new(result.Order!.Id, result.Order.Status, result.EventId!);
}
