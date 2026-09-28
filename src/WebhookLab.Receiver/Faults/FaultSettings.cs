namespace WebhookLab.Receiver.Faults;

public sealed record FaultSnapshot(int FailNext, int SlowNext, int CrashNext, bool DedupeOff);

/// <summary>
/// Receiving-side failures you can switch on from the dashboard. Counters are one-shot:
/// "FailNext = 3" fails the next three requests, then turns itself off.
/// Thread-safe because requests and the background processor use it concurrently.
/// </summary>
public sealed class FaultSettings
{
    private int _failNext;
    private int _slowNext;
    private int _crashNext;
    private volatile bool _dedupeOff;

    /// <summary>How late a "slow" response is. Longer than the Provider's 5 s timeout by default.</summary>
    public TimeSpan SlowResponseDelay { get; init; } = TimeSpan.FromSeconds(6);

    /// <summary>UNSAFE lab switch: duplicates are processed again instead of skipped.</summary>
    public bool DedupeOff => _dedupeOff;

    public FaultSnapshot Snapshot() =>
        new(Volatile.Read(ref _failNext), Volatile.Read(ref _slowNext), Volatile.Read(ref _crashNext), _dedupeOff);

    public void Apply(FaultSnapshot faults)
    {
        Volatile.Write(ref _failNext, faults.FailNext);
        Volatile.Write(ref _slowNext, faults.SlowNext);
        Volatile.Write(ref _crashNext, faults.CrashNext);
        _dedupeOff = faults.DedupeOff;
    }

    public void Reset() => Apply(new FaultSnapshot(0, 0, 0, false));

    public bool TryConsumeFail() => TryConsume(ref _failNext);
    public bool TryConsumeSlow() => TryConsume(ref _slowNext);
    public bool TryConsumeCrash() => TryConsume(ref _crashNext);

    private static bool TryConsume(ref int counter)
    {
        while (true)
        {
            var current = Volatile.Read(ref counter);
            if (current <= 0)
                return false;
            if (Interlocked.CompareExchange(ref counter, current - 1, current) == current)
                return true;
        }
    }
}
