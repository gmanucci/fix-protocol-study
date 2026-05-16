using System.Threading.Channels;
using Fix.Consumer;
using Fix.Protocol;
using Microsoft.Extensions.Logging;

namespace Fix.Consumer.QuickFix;

/// <summary>
/// <see cref="IMarketConnection"/> backed by the shared <see cref="QuickFixApplication"/>.
/// Each instance corresponds to one (symbol, subscriber) pair; <see cref="StartAsync"/>
/// registers the channel with the application, and <see cref="DisposeAsync"/> removes it.
/// </summary>
public sealed class QuickFixMarketConnection : IMarketConnection
{
    private readonly QuickFixApplication _app;
    private readonly Channel<FixEvent> _channel;
    private readonly string _key;

    public QuickFixMarketConnection(string symbol, QuickFixApplication app, int channelCapacity)
    {
        Symbol = symbol;
        _app = app;
        _key = $"{symbol}|{Guid.NewGuid():N}";
        _channel = Channel.CreateBounded<FixEvent>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
    }

    public string Symbol { get; }
    public string Transport => "QuickFIX";
    public ChannelReader<FixEvent> Events => _channel.Reader;

    public Task StartAsync(IReadOnlySet<FixEventKind> subscribedKinds, CancellationToken ct)
    {
        var kinds = subscribedKinds.Count > 0
            ? subscribedKinds
            : new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh };
        _app.Register(_key, Symbol, kinds, _channel.Writer);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _app.Unregister(_key);
        _channel.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
