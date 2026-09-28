using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Api;

public static class QueryEndpoints
{
    public static IEndpointRouteBuilder MapQueryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/inbox", async (ReceiverDbContext db) =>
            await db.InboxEvents.AsNoTracking().OrderByDescending(e => e.ReceivedAt).ToListAsync());

        app.MapGet("/api/ledger", async (ReceiverDbContext db) =>
            await db.LedgerOrders.AsNoTracking().OrderBy(o => o.OrderId).ToListAsync());

        return app;
    }
}
