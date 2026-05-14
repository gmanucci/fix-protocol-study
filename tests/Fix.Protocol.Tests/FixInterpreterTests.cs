using System.Text;
using Fix.Protocol;
using Xunit;

namespace Fix.Protocol.Tests;

public class FixInterpreterTests
{
    [Fact]
    public void IncrementalRefresh_RoundTrip_PreservesAllFields()
    {
        var symbol = "EURUSD"u8;
        var original = new MarketTick(symbol, price: 1.10532, quantity: 1_500_000, timestampTicks: 638_000_000_000_000_000L, entryType: MdEntryType.Bid);

        Span<byte> buffer = stackalloc byte[256];
        Assert.True(FixInterpreter.TryWriteIncrementalRefresh(in original, buffer, out var written));
        Assert.True(written > 0);

        var encoded = buffer[..written];
        Assert.True(FixInterpreter.TryParseIncrementalRefresh(encoded, out var parsed));

        Assert.Equal("EURUSD", parsed.Symbol);
        Assert.Equal(original.Price, parsed.Price, 5);
        Assert.Equal(original.Quantity, parsed.Quantity, 5);
        Assert.Equal(original.TimestampTicks, parsed.TimestampTicks);
        Assert.Equal(original.EntryType, parsed.EntryType);
    }

    [Fact]
    public void Encoded_Message_ContainsHeaderTrailerAndBodyLength()
    {
        var tick = new MarketTick("AAPL"u8, 150.25, 100, DateTime.UtcNow.Ticks, MdEntryType.Trade);
        Span<byte> buffer = stackalloc byte[256];
        Assert.True(FixInterpreter.TryWriteIncrementalRefresh(in tick, buffer, out var written));
        var s = Encoding.ASCII.GetString(buffer[..written]).Replace('\u0001', '|');
        Assert.StartsWith("8=FIX.4.4|9=", s);
        Assert.Contains("|35=X|", s);
        Assert.Contains("|55=AAPL|", s);
        Assert.EndsWith("|", s);
        // Trailer must end with 10=NNN|
        Assert.Matches(@"\|10=\d{3}\|$", s);
    }

    [Fact]
    public void TryFindMessageBoundary_FindsCompleteMessage()
    {
        var tick = new MarketTick("MSFT"u8, 300.0, 50, 1234567890L, MdEntryType.Offer);
        Span<byte> buffer = stackalloc byte[256];
        Assert.True(FixInterpreter.TryWriteIncrementalRefresh(in tick, buffer, out var written));

        var len = FixInterpreter.TryFindMessageBoundary(buffer[..written]);
        Assert.Equal(written, len);
    }

    [Fact]
    public void TryFindMessageBoundary_ReturnsZeroForPartial()
    {
        var tick = new MarketTick("MSFT"u8, 300.0, 50, 1234567890L, MdEntryType.Offer);
        Span<byte> buffer = stackalloc byte[256];
        Assert.True(FixInterpreter.TryWriteIncrementalRefresh(in tick, buffer, out var written));
        // Truncate before the trailer.
        Assert.Equal(0, FixInterpreter.TryFindMessageBoundary(buffer[..(written - 4)]));
    }

    [Fact]
    public void TryParseIncrementalRefresh_RejectsLogon()
    {
        Span<byte> buffer = stackalloc byte[128];
        Assert.True(FixInterpreter.TryWriteLogon("EURUSD"u8, buffer, out var written));
        Assert.False(FixInterpreter.TryParseIncrementalRefresh(buffer[..written], out _));
    }

    [Fact]
    public void Logon_GetSymbol_RoundTrip()
    {
        Span<byte> buffer = stackalloc byte[128];
        Assert.True(FixInterpreter.TryWriteLogon("GBPJPY"u8, buffer, out var written));

        Assert.True(FixInterpreter.TryGetMsgType(buffer[..written], out var msgType));
        Assert.Equal(FixConstants.MsgTypeLogon, msgType);

        Span<byte> sym = stackalloc byte[16];
        Assert.True(FixInterpreter.TryGetSymbol(buffer[..written], sym, out var symLen));
        Assert.Equal("GBPJPY", Encoding.ASCII.GetString(sym[..symLen]));
    }

    [Fact]
    public void Checksum_IsModulo256OfBytes()
    {
        ReadOnlySpan<byte> data = "8=FIX.4.4\u00019=12\u000135=A\u0001"u8;
        var expected = 0;
        for (int i = 0; i < data.Length; i++) expected += data[i];
        Assert.Equal((byte)(expected % 256), FixInterpreter.ComputeChecksum(data));
    }
}
