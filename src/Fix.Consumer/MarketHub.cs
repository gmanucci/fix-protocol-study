using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

namespace Fix.Consumer;

/// <summary>
/// Plain DTO sent over SignalR. Records are fine here — this is a cold-ish path that crosses
/// the JSON serializer anyway, and the producer-side hot path keeps using <c>MarketTick</c>
/// structs internally.
/// </summary>
public sealed record TickDto(string Symbol, double Price, double Quantity, long TimestampTicks, string EntryType, string Transport);

/// <summary>
/// SignalR hub. Clients call <c>Subscribe</c> / <c>Unsubscribe</c> and then receive ticks via the
/// "tick" callback. One <see cref="IMarketConnection"/> is created per (caller, symbol, transport).
/// </summary>
public sealed class MarketHub : Hub
{
    // Per-connection map of (symbol+transport → market connection + pump task)
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Subscription>> Subs = new();

    private readonly MarketConnectionFactory _factory;
    private readonly ILogger<MarketHub> _logger;

    public MarketHub(MarketConnectionFactory factory, ILogger<MarketHub> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public async Task Subscribe(string symbol, string transport)
    {
        if (string.IsNullOrWhiteSpace(symbol)) throw new HubException("symbol required");
        if (!Enum.TryParse<Transport>(transport, ignoreCase: true, out var t))
            throw new HubException($"unknown transport '{transport}'");

        var key = $"{symbol}|{t}";
        var bag = Subs.GetOrAdd(Context.ConnectionId, _ => new ConcurrentDictionary<string, Subscription>());

        if (bag.ContainsKey(key)) return;

        var conn = _factory.Create(symbol, t);
        await conn.StartAsync(Context.ConnectionAborted).ConfigureAwait(false);

        var cts = CancellationTokenSource.CreateLinkedTokenSource(Context.ConnectionAborted);
        var sub = new Subscription(conn, cts);
        if (!bag.TryAdd(key, sub))
        {
            await conn.DisposeAsync();
            cts.Dispose();
            return;
        }

        var caller = Clients.Caller;
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var tick in conn.Ticks.ReadAllAsync(cts.Token).ConfigureAwait(false))
                {
                    var dto = new TickDto(
                        tick.Symbol,
                        tick.Price,
                        tick.Quantity,
                        tick.TimestampTicks,
                        EntryTypeToString(tick.EntryType),
                        conn.Transport);
                    await caller.SendAsync("tick", dto, cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Pump for {Symbol} ended", symbol);
            }
        }, cts.Token);
    }

    public async Task Unsubscribe(string symbol, string transport)
    {
        if (!Enum.TryParse<Transport>(transport, ignoreCase: true, out var t)) return;
        var key = $"{symbol}|{t}";
        if (Subs.TryGetValue(Context.ConnectionId, out var bag) && bag.TryRemove(key, out var sub))
        {
            await sub.DisposeAsync();
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        if (Subs.TryRemove(Context.ConnectionId, out var bag))
        {
            foreach (var sub in bag.Values)
            {
                await sub.DisposeAsync();
            }
        }
        await base.OnDisconnectedAsync(exception);
    }

    private static string EntryTypeToString(byte b) => b switch
    {
        Fix.Protocol.MdEntryType.Bid => "Bid",
        Fix.Protocol.MdEntryType.Offer => "Offer",
        Fix.Protocol.MdEntryType.Trade => "Trade",
        _ => "Unknown",
    };

    private sealed class Subscription : IAsyncDisposable
    {
        private readonly IMarketConnection _conn;
        private readonly CancellationTokenSource _cts;
        public Subscription(IMarketConnection conn, CancellationTokenSource cts)
        {
            _conn = conn;
            _cts = cts;
        }
        public async ValueTask DisposeAsync()
        {
            try { _cts.Cancel(); } catch { }
            await _conn.DisposeAsync();
            _cts.Dispose();
        }
    }
}
