namespace WebhookLab.Provider.Chaos;

/// <summary>
/// Deliveries held back by the ReorderNext chaos setting. Once the last one is collected, the
/// dispatcher releases them all newest-first — so the Receiver sees events in the wrong order.
/// </summary>
public sealed class ReorderBuffer
{
    private readonly Lock _gate = new();
    private readonly List<int> _held = [];

    public bool HasHeld
    {
        get { lock (_gate) return _held.Count > 0; }
    }

    public void Hold(int deliveryId)
    {
        lock (_gate) _held.Add(deliveryId);
    }

    public bool IsHeld(int deliveryId)
    {
        lock (_gate) return _held.Contains(deliveryId);
    }

    /// <summary>Returns the held delivery ids, newest first, and empties the buffer.</summary>
    public IReadOnlyList<int> ReleaseReversed()
    {
        lock (_gate)
        {
            var released = new List<int>(_held);
            released.Reverse();
            _held.Clear();
            return released;
        }
    }

    public void Clear()
    {
        lock (_gate) _held.Clear();
    }
}
