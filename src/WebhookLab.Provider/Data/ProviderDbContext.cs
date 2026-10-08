using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace WebhookLab.Provider.Data;

public sealed class ProviderDbContext(DbContextOptions<ProviderDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<WebhookEvent> Events => Set<WebhookEvent>();
    public DbSet<Delivery> Deliveries => Set<Delivery>();
    public DbSet<DeliveryAttempt> DeliveryAttempts => Set<DeliveryAttempt>();

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        // Same fix as the Receiver: SQLite hands DateTime back as Unspecified, which JSON writes without
        // a trailing "Z". We only ever store UTC, so stamp the Kind back on read. Stored values are untouched.
        configurationBuilder.Properties<DateTime>().HaveConversion<UtcDateTimeConverter>();
    }

    private sealed class UtcDateTimeConverter() : ValueConverter<DateTime, DateTime>(
        v => v,
        v => DateTime.SpecifyKind(v, DateTimeKind.Utc));

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(order => order.Property(o => o.Status).HasConversion<string>());

        modelBuilder.Entity<WebhookEvent>(webhookEvent => webhookEvent.ToTable("Events"));

        modelBuilder.Entity<Delivery>(delivery =>
        {
            delivery.Property(d => d.Status).HasConversion<string>();
            // The dispatcher's "what's due?" query filters on these two columns every second.
            delivery.HasIndex(d => new { d.Status, d.NextAttemptAt });
            delivery.HasOne<WebhookEvent>().WithMany().HasForeignKey(d => d.EventId);
            delivery.HasMany(d => d.Attempts).WithOne().HasForeignKey(a => a.DeliveryId);
        });
    }
}
