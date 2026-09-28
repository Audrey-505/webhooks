using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Tests;

/// <summary>An in-memory SQLite database that lives as long as this object.</summary>
public sealed class TestDb : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    public TestDb()
    {
        _connection.Open();
        Context = new ReceiverDbContext(new DbContextOptionsBuilder<ReceiverDbContext>().UseSqlite(_connection).Options);
        Context.Database.EnsureCreated();
    }

    public ReceiverDbContext Context { get; }

    public void Dispose()
    {
        Context.Dispose();
        _connection.Dispose();
    }
}
