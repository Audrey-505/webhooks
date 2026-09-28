using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace WebhookLab.Receiver.Data;

public sealed class ReceiverDbContext(DbContextOptions<ReceiverDbContext> options) : DbContext(options)
{
    public DbSet<InboxEvent> InboxEvents => Set<InboxEvent>();
    public DbSet<LedgerOrder> LedgerOrders => Set<LedgerOrder>();

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        // SQLite has no DateTimeKind of its own, so EF Core reads every DateTime back as Unspecified,
        // which System.Text.Json then serializes with no trailing "Z" (browsers read it as local time).
        // We only ever store UTC, so stamp the Kind back on read, model-wide, instead of fixing it up
        // at every call site. The stored value is untouched, so ordering/comparison in InboxProcessor
        // (e.g. NextAttemptAt <= now) keep working exactly as before.
        configurationBuilder.Properties<DateTime>().HaveConversion<UtcDateTimeConverter>();
    }

    private sealed class UtcDateTimeConverter() : ValueConverter<DateTime, DateTime>(
        v => v,
        v => DateTime.SpecifyKind(v, DateTimeKind.Utc));

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
