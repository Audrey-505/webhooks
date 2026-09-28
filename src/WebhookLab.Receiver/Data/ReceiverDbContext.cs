using Microsoft.EntityFrameworkCore;

namespace WebhookLab.Receiver.Data;

public sealed class ReceiverDbContext(DbContextOptions<ReceiverDbContext> options) : DbContext(options)
{
    public DbSet<InboxEvent> InboxEvents => Set<InboxEvent>();
    public DbSet<LedgerOrder> LedgerOrders => Set<LedgerOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InboxEvent>(inbox =>
        {
            // The idempotency guarantee: inserting the same event id twice fails at the database.
            inbox.HasKey(e => e.EventId);
            inbox.Property(e => e.Status).HasConversion<string>();
            inbox.HasIndex(e => new { e.Status, e.NextAttemptAt });
        });

        modelBuilder.Entity<LedgerOrder>(order =>
        {
            order.HasKey(o => o.OrderId);
            order.Property(o => o.OrderId).ValueGeneratedNever();
            order.Property(o => o.Status).HasConversion<string>();
        });
    }
}
