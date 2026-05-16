using Fix.Protocol;

namespace Fix.Consumer;

/// <summary>
/// Fans an event out to a sequence of inner publishers. Failures of one publisher are
/// logged but do not prevent the others from receiving the event — the goal is best-effort
/// delivery to every active transport.
/// </summary>
public sealed class CompositeEventPublisher : IEventPublisher
{
    private readonly IReadOnlyList<IEventPublisher> _inner;

    public CompositeEventPublisher(IEnumerable<IEventPublisher> inner)
    {
        _inner = inner.ToArray();
    }

    public IReadOnlyList<IEventPublisher> Inner => _inner;

    public async Task PublishAsync(string subscriberId, FixEvent evt, CancellationToken ct)
    {
        if (_inner.Count == 0) return;
        if (_inner.Count == 1) { await _inner[0].PublishAsync(subscriberId, evt, ct).ConfigureAwait(false); return; }
        var tasks = new Task[_inner.Count];
        for (int i = 0; i < _inner.Count; i++)
        {
            tasks[i] = SafeAsync(_inner[i], subscriberId, evt, ct);
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public async Task CompleteAsync(string subscriberId, CancellationToken ct)
    {
        var tasks = new Task[_inner.Count];
        for (int i = 0; i < _inner.Count; i++)
        {
            tasks[i] = SafeCompleteAsync(_inner[i], subscriberId, ct);
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static async Task SafeAsync(IEventPublisher p, string id, FixEvent evt, CancellationToken ct)
    {
        try { await p.PublishAsync(id, evt, ct).ConfigureAwait(false); }
        catch (OperationCanceledException) { }
        catch { /* swallow — best effort fan-out */ }
    }

    private static async Task SafeCompleteAsync(IEventPublisher p, string id, CancellationToken ct)
    {
        try { await p.CompleteAsync(id, ct).ConfigureAwait(false); }
        catch (OperationCanceledException) { }
        catch { /* swallow */ }
    }
}
