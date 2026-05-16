using Fix.Protocol;
using Microsoft.Extensions.Logging;

namespace Fix.Consumer;

/// <summary>
/// Drains <see cref="IMarketConnection.Events"/> and forwards every <see cref="FixEvent"/> to
/// an <see cref="IEventPublisher"/>. Owns the lifecycle of both the connection and a linked
/// <see cref="CancellationTokenSource"/>; disposing the pump tears everything down cleanly.
/// </summary>
public sealed class SubscriptionPump : IAsyncDisposable
{
    private readonly IMarketConnection _connection;
    private readonly IEventPublisher _publisher;
    private readonly string _subscriberId;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private Task? _runner;

    public SubscriptionPump(
        IMarketConnection connection,
        IEventPublisher publisher,
        string subscriberId,
        ILogger logger,
        CancellationToken linkedTo)
    {
        _connection = connection;
        _publisher = publisher;
        _subscriberId = subscriberId;
        _logger = logger;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(linkedTo);
    }

    public IMarketConnection Connection => _connection;

    public Task StartAsync(IReadOnlySet<FixEventKind> kinds)
    {
        _runner = Task.Run(() => RunAsync(kinds, _cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    private async Task RunAsync(IReadOnlySet<FixEventKind> kinds, CancellationToken ct)
    {
        try
        {
            await _connection.StartAsync(kinds, ct).ConfigureAwait(false);
            await foreach (var evt in _connection.Events.ReadAllAsync(ct).ConfigureAwait(false))
            {
                await _publisher.PublishAsync(_subscriberId, evt, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Pump for subscriber {Subscriber} symbol {Symbol} ended", _subscriberId, _connection.Symbol);
        }
        finally
        {
            try { await _publisher.CompleteAsync(_subscriberId, CancellationToken.None).ConfigureAwait(false); }
            catch { /* best effort */ }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try { _cts.Cancel(); } catch { }
        try { await _connection.DisposeAsync().ConfigureAwait(false); } catch { }
        if (_runner is not null)
        {
            try { await _runner.ConfigureAwait(false); } catch { }
        }
        _cts.Dispose();
    }
}
