namespace WebhookLab.Provider.Dispatch;

/// <summary>Runs the delivery dispatcher once a second for the lifetime of the app.</summary>
public sealed class DeliveryDispatcherWorker(IServiceScopeFactory scopes, ILogger<DeliveryDispatcherWorker> logger) : BackgroundService
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
                await scope.ServiceProvider.GetRequiredService<DeliveryDispatcher>().DispatchDueAsync(stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // A stray failure (e.g. a database error) must not stop the host; only shutdown ends this loop.
                logger.LogError(ex, "Delivery dispatch pass failed; trying again next tick");
            }
        }
    }
}
