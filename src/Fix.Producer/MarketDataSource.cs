using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Fix.Protocol;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Fix.Producer;

/// <summary>
/// Singleton mock market data source. One background task per symbol writes ticks (random walk)
/// into a bounded channel with <see cref="BoundedChannelFullMode.DropOldest"/> so producers
/// never block when subscribers fall behind.
/// </summary>
public sealed class MarketDataSource : BackgroundService, IMarketDataSource
{
    private readonly ILogger<MarketDataSource> _logger;
    private readonly ProducerOptions _options;
    private readonly Dictionary<string, Channel<MarketTick>> _channels;
    private readonly Dictionary<string, byte[]> _symbolBytes;

    public MarketDataSource(IOptions<ProducerOptions> options, ILogger<MarketDataSource> logger)
    {
        _logger = logger;
        _options = options.Value;
        _channels = new(_options.Markets.Length, StringComparer.Ordinal);
        _symbolBytes = new(_options.Markets.Length, StringComparer.Ordinal);
        foreach (var s in _options.Markets)
        {
            _channels[s] = Channel.CreateBounded<MarketTick>(new BoundedChannelOptions(_options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = false,
                SingleWriter = true,
                AllowSynchronousContinuations = false,
            });
            _symbolBytes[s] = Encoding.ASCII.GetBytes(s);
        }
    }

    public IReadOnlyList<string> Symbols => _options.Markets;

    public ChannelReader<MarketTick>? GetReader(string symbol) =>
        _channels.TryGetValue(symbol, out var ch) ? ch.Reader : null;

    public ChannelReader<MarketTick>? GetReader(ReadOnlySpan<byte> symbol)
    {
        // Linear scan: number of markets is small (typically dozens). Avoids string alloc.
        foreach (var (name, bytes) in _symbolBytes)
        {
            if (bytes.AsSpan().SequenceEqual(symbol))
                return _channels[name].Reader;
        }
        return null;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tasks = new Task[_options.Markets.Length];
        for (int i = 0; i < _options.Markets.Length; i++)
        {
            var symbol = _options.Markets[i];
            var seed = symbol.GetHashCode() ^ Environment.TickCount;
            tasks[i] = Task.Run(() => RunAsync(symbol, seed, stoppingToken), stoppingToken);
        }
        return Task.WhenAll(tasks);
    }

    private async Task RunAsync(string symbol, int seed, CancellationToken ct)
    {
        var symbolBytes = _symbolBytes[symbol];
        var writer = _channels[symbol].Writer;
        var rng = new Random(seed);
        double price = 100.0 + rng.NextDouble() * 100.0;
        var intervalUs = _options.TickIntervalMicroseconds;
        var intervalTicks = intervalUs <= 0 ? 0L : intervalUs * (Stopwatch.Frequency / 1_000_000L);

        _logger.LogInformation("Market {Symbol} starting at {Price:F4} (interval {IntervalUs}us)", symbol, price, intervalUs);

        var nextDue = Stopwatch.GetTimestamp();

        while (!ct.IsCancellationRequested)
        {
            // Random walk
            price += (rng.NextDouble() - 0.5) * 0.05;
            if (price < 1) price = 1;
            var qty = 100.0 + rng.Next(0, 1000);
            var entryType = (rng.Next(0, 3)) switch
            {
                0 => MdEntryType.Bid,
                1 => MdEntryType.Offer,
                _ => MdEntryType.Trade,
            };
            var tick = new MarketTick(symbolBytes, price, qty, DateTime.UtcNow.Ticks, entryType);

            // DropOldest semantics → TryWrite always succeeds.
            writer.TryWrite(tick);

            if (intervalTicks <= 0) continue;
            nextDue += intervalTicks;
            var remaining = nextDue - Stopwatch.GetTimestamp();
            if (remaining > 0)
            {
                var ms = (int)(remaining * 1000L / Stopwatch.Frequency);
                if (ms > 0)
                {
                    try { await Task.Delay(ms, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }
                }
            }
            else if (-remaining > intervalTicks * 8)
            {
                // We're way behind — re-baseline so we don't busy-spin trying to catch up.
                nextDue = Stopwatch.GetTimestamp();
            }
        }

        writer.TryComplete();
    }
}
