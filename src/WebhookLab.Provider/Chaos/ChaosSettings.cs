namespace WebhookLab.Provider.Chaos;

public sealed record ChaosSnapshot(int DuplicateNext, int DelaySeconds, int ReorderNext, int BadSignatureNext);

/// <summary>
/// Sending-side failures you can switch on from the dashboard — the Provider's mirror of the
/// Receiver's FaultSettings. Counters are one-shot ("DuplicateNext = 2" duplicates the next two
/// deliveries, then turns itself off); DelaySeconds applies before each delivery attempt (a
/// DuplicateNext copy follows immediately, with no extra delay) and stays set until changed.
/// Thread-safe because the API and the dispatcher use it concurrently.
/// </summary>
public sealed class ChaosSettings
{
    private int _duplicateNext;
    private int _delaySeconds;
    private int _reorderNext;
    private int _badSignatureNext;

    /// <summary>How long to wait before each send.</summary>
    public TimeSpan Delay => TimeSpan.FromSeconds(Volatile.Read(ref _delaySeconds));

    /// <summary>True while ReorderNext is still collecting deliveries to hold back.</summary>
    public bool ReorderPending => Volatile.Read(ref _reorderNext) > 0;

    public ChaosSnapshot Snapshot() => new(
        Volatile.Read(ref _duplicateNext),
        Volatile.Read(ref _delaySeconds),
        Volatile.Read(ref _reorderNext),
        Volatile.Read(ref _badSignatureNext));

    public void Apply(ChaosSnapshot chaos)
    {
        // Negative numbers mean nothing here; store them as "off" so the API never reports nonsense.
        Volatile.Write(ref _duplicateNext, Math.Max(0, chaos.DuplicateNext));
        // Capped at 60 s: an unbounded delay would freeze the dispatcher's one-second poll loop for
        // everyone, not just this delivery, and a huge value risks overflowing Task.Delay's timer.
        Volatile.Write(ref _delaySeconds, Math.Clamp(chaos.DelaySeconds, 0, 60));
        Volatile.Write(ref _reorderNext, Math.Max(0, chaos.ReorderNext));
        Volatile.Write(ref _badSignatureNext, Math.Max(0, chaos.BadSignatureNext));
    }

    public void Reset() => Apply(new ChaosSnapshot(0, 0, 0, 0));

    public bool TryConsumeDuplicate() => TryConsume(ref _duplicateNext);
    public bool TryConsumeReorder() => TryConsume(ref _reorderNext);
    public bool TryConsumeBadSignature() => TryConsume(ref _badSignatureNext);

    // Same compare-and-swap loop as the Receiver's FaultSettings: two threads can't both use the last one.
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
