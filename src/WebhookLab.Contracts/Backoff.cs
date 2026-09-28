namespace WebhookLab.Contracts;

/// <summary>
/// Exponential backoff shared by the Provider's delivery retries and the Receiver's deferred events.
/// Jitter spreads retries out so many failed deliveries don't all retry at the same instant.
/// </summary>
public static class Backoff
{
    public const int MaxAttempts = 5;

    private static readonly TimeSpan MaxJitter = TimeSpan.FromMilliseconds(500);

    /// <param name="attemptCount">Attempts made so far (1 after the first failure → ~2 s).</param>
    public static TimeSpan Delay(int attemptCount, Random random) =>
        TimeSpan.FromSeconds(Math.Pow(2, attemptCount)) + MaxJitter * random.NextDouble();
}
