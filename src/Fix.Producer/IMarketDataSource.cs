using System.Threading.Channels;
using Fix.Protocol;

namespace Fix.Producer;

/// <summary>
/// Singleton source of mock market data. Holds one bounded channel per symbol; transports
/// subscribe by reading from <see cref="GetReader"/>.
/// </summary>
public interface IMarketDataSource
{
    IReadOnlyList<string> Symbols { get; }
    /// <summary>Returns a reader for the given symbol, or null if unknown.</summary>
    ChannelReader<MarketTick>? GetReader(ReadOnlySpan<byte> symbol);
    ChannelReader<MarketTick>? GetReader(string symbol);
}
