using Fix.Protocol;

namespace Fix.Consumer;

/// <summary>
/// Helpers for wrapping <see cref="MarketTick"/> values from the existing study-grade
/// raw-byte transports (TCP / UDP) into <see cref="FixEvent"/> envelopes that the new
/// pluggable publisher pipeline expects.
/// </summary>
public static class MarketTickEvents
{
    public static FixEvent ToIncrementalRefresh(in MarketTick tick, string source) => new(
        Kind: FixEventKind.MarketDataIncrementalRefresh,
        Symbol: tick.Symbol,
        SenderCompId: null,
        TargetCompId: null,
        MsgSeqNum: null,
        SendingTimeTicks: tick.TimestampTicks,
        Source: source,
        Payload: new MarketDataPayload(
            Price: tick.Price,
            Quantity: tick.Quantity,
            EntryType: EntryTypeToString(tick.EntryType)));

    public static string EntryTypeToString(byte b) => b switch
    {
        MdEntryType.Bid => "Bid",
        MdEntryType.Offer => "Offer",
        MdEntryType.Trade => "Trade",
        _ => "Unknown",
    };
}
