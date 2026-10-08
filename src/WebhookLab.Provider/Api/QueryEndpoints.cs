using Microsoft.EntityFrameworkCore;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Api;

public static class QueryEndpoints
{
    public static IEndpointRouteBuilder MapQueryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/orders", async (ProviderDbContext db) =>
            await db.Orders.AsNoTracking().OrderBy(o => o.Id).ToListAsync());

        app.MapGet("/api/deliveries", async (ProviderDbContext db) =>
        {
            var deliveries = await db.Deliveries.AsNoTracking()
                .Include(d => d.Attempts)
                .OrderByDescending(d => d.Id)
                .ToListAsync();
            var eventTypes = await db.Events.AsNoTracking().ToDictionaryAsync(e => e.Id, e => e.Type);
            return deliveries.Select(d => DeliveryView.From(d, eventTypes[d.EventId]));
        });

        return app;
    }
}
