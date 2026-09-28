namespace WebhookLab.Receiver.Processing;

/// <summary>Runs the inbox processor once a second for the lifetime of the app.</summary>
public sealed class InboxProcessorWorker(IServiceScopeFactory scopes, ILogger<InboxProcessorWorker> logger) : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(PollInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                using var scope = scopes.CreateScope();
                await scope.ServiceProvider.GetRequiredService<InboxProcessor>().ProcessDueAsync(stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // A stray OperationCanceledException not caused by shutdown (e.g. a query timeout)
                // must not stop the host; only a real shutdown request should end this loop.
                logger.LogError(ex, "Inbox processing pass failed; trying again next tick");
            }
        }
    }
}
