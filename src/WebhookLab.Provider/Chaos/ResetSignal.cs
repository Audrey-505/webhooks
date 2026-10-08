namespace WebhookLab.Provider.Chaos;

/// <summary>
/// Lets POST /api/reset tell an in-flight delivery "stop — your row may be gone". Trigger()
/// atomically swaps in a fresh CancellationTokenSource and cancels the old one; the dispatcher
/// reads Token at the start of each delivery and links it with its own CancellationToken, so a
/// reset mid-delay or mid-send is observable without the dispatcher polling anything.
/// We never Dispose the swapped-out source: a delivery already linked to its token may still be
/// running, and disposing would make that token throw ObjectDisposedException instead of just
/// reporting itself cancelled. Letting it get garbage-collected is fine — it holds no other resources.
/// </summary>
public sealed class ResetSignal
{
    private CancellationTokenSource _cts = new();

    public CancellationToken Token => _cts.Token;

    public void Trigger()
    {
        var old = Interlocked.Exchange(ref _cts, new CancellationTokenSource());
        old.Cancel();
    }
}
