using System.Text.Json;
using Fix.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Fix.Consumer;

/// <summary>
/// Publishes <see cref="FixEvent"/> envelopes to a Redis pub/sub channel named
/// <c>{Prefix}.{subscriberId}</c>. Other processes can subscribe to receive the JSON payload.
/// </summary>
/// <remarks>
/// The connection is created lazily on first publish so the consumer process boots even when
/// Redis is unreachable; failures are logged and swallowed (publisher contract is best-effort).
/// </remarks>
public sealed class RedisEventPublisher : IEventPublisher, IAsyncDisposable
{
    private readonly RedisPublisherOptions _options;
    private readonly ILogger<RedisEventPublisher> _logger;
    private readonly SemaphoreSlim _connectGate = new(1, 1);
    private IConnectionMultiplexer? _mux;

    public RedisEventPublisher(IOptions<ConsumerOptions> options, ILogger<RedisEventPublisher> logger)
    {
        _options = options.Value.Redis;
        _logger = logger;
    }

    /// <summary>Test seam: inject a pre-built multiplexer (e.g. fakes / Testcontainers).</summary>
    public RedisEventPublisher(IConnectionMultiplexer mux, RedisPublisherOptions options, ILogger<RedisEventPublisher> logger)
    {
        _mux = mux;
        _options = options;
        _logger = logger;
    }

    public async Task PublishAsync(string subscriberId, FixEvent evt, CancellationToken ct)
    {
        var mux = await EnsureConnectedAsync(ct).ConfigureAwait(false);
        if (mux is null) return;
        var channel = $"{_options.ChannelPrefix}.{subscriberId}";
        var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
        await mux.GetSubscriber().PublishAsync(RedisChannel.Literal(channel), payload).ConfigureAwait(false);
    }

    public Task CompleteAsync(string subscriberId, CancellationToken ct) => Task.CompletedTask;

    private async Task<IConnectionMultiplexer?> EnsureConnectedAsync(CancellationToken ct)
    {
        if (_mux is { IsConnected: true }) return _mux;
        await _connectGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_mux is { IsConnected: true }) return _mux;
            try
            {
                _mux = await ConnectionMultiplexer.ConnectAsync(_options.Configuration).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Redis connect to {Configuration} failed", _options.Configuration);
                _mux = null;
            }
            return _mux;
        }
        finally
        {
            _connectGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_mux is not null) await _mux.DisposeAsync().ConfigureAwait(false);
        _connectGate.Dispose();
    }
}
