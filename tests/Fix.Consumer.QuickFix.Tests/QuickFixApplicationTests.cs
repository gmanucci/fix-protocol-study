using System.Threading.Channels;
using Fix.Consumer.QuickFix;
using Fix.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using QuickFix;
using QuickFix.Fields;

namespace Fix.Consumer.QuickFix.Tests;

public class QuickFixApplicationTests
{
    [Fact]
    public void Dispatch_writes_market_data_incremental_refresh_event_for_matching_symbol()
    {
        var app = new QuickFixApplication(NullLogger.Instance);
        var ch = Channel.CreateUnbounded<FixEvent>();
        app.Register("sub-1", "EURUSD", new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh }, ch.Writer);

        var msg = NewIncrementalRefresh("EURUSD", price: 1.2345, qty: 1000, type: "0");
        app.Dispatch(msg);

        Assert.True(ch.Reader.TryRead(out var evt));
        Assert.Equal(FixEventKind.MarketDataIncrementalRefresh, evt!.Kind);
        Assert.Equal("EURUSD", evt.Symbol);
        var md = Assert.IsType<MarketDataPayload>(evt.Payload);
        Assert.Equal(1.2345, md.Price);
        Assert.Equal(1000, md.Quantity);
        Assert.Equal("Bid", md.EntryType);
    }

    [Fact]
    public void Dispatch_filters_by_symbol()
    {
        var app = new QuickFixApplication(NullLogger.Instance);
        var ch = Channel.CreateUnbounded<FixEvent>();
        app.Register("sub-1", "EURUSD", new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh }, ch.Writer);

        app.Dispatch(NewIncrementalRefresh("GBPUSD", 1.0, 1, "0"));

        Assert.False(ch.Reader.TryRead(out _));
    }

    [Fact]
    public void Dispatch_filters_by_event_kind()
    {
        var app = new QuickFixApplication(NullLogger.Instance);
        var ch = Channel.CreateUnbounded<FixEvent>();
        // Subscriber only wants Logon
        app.Register("sub-1", "*", new HashSet<FixEventKind> { FixEventKind.Logon }, ch.Writer);

        app.Dispatch(NewIncrementalRefresh("EURUSD", 1.0, 1, "0"));
        Assert.False(ch.Reader.TryRead(out _));

        app.Dispatch(NewAdmin("A"));
        Assert.True(ch.Reader.TryRead(out var evt));
        Assert.Equal(FixEventKind.Logon, evt!.Kind);
    }

    [Theory]
    [InlineData("0", FixEventKind.Heartbeat)]
    [InlineData("1", FixEventKind.TestRequest)]
    [InlineData("2", FixEventKind.ResendRequest)]
    [InlineData("3", FixEventKind.Reject)]
    [InlineData("4", FixEventKind.SequenceReset)]
    [InlineData("5", FixEventKind.Logout)]
    [InlineData("A", FixEventKind.Logon)]
    [InlineData("B", FixEventKind.News)]
    [InlineData("Y", FixEventKind.MarketDataRequestReject)]
    [InlineData("j", FixEventKind.BusinessMessageReject)]
    public void Dispatch_maps_every_supported_msg_type(string msgType, FixEventKind expected)
    {
        var app = new QuickFixApplication(NullLogger.Instance);
        var ch = Channel.CreateUnbounded<FixEvent>();
        app.Register("sub-1", "*", new HashSet<FixEventKind>(FixEventKinds.All), ch.Writer);

        app.Dispatch(NewAdmin(msgType));

        Assert.True(ch.Reader.TryRead(out var evt));
        Assert.Equal(expected, evt!.Kind);
        Assert.Equal("QuickFIX", evt.Source);
    }

    [Fact]
    public void Unregister_completes_writer_and_stops_dispatch()
    {
        var app = new QuickFixApplication(NullLogger.Instance);
        var ch = Channel.CreateUnbounded<FixEvent>();
        app.Register("sub-1", "EURUSD", new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh }, ch.Writer);
        app.Unregister("sub-1");

        app.Dispatch(NewIncrementalRefresh("EURUSD", 1, 1, "0"));

        Assert.False(ch.Reader.TryRead(out _));
        Assert.True(ch.Reader.Completion.IsCompleted);
    }

    private static Message NewIncrementalRefresh(string symbol, double price, double qty, string type)
    {
        var m = new Message();
        m.Header.SetField(new MsgType("X"));
        m.Header.SetField(new SenderCompID("FIX_PRODUCER"));
        m.Header.SetField(new TargetCompID("FIX_CONSUMER"));
        m.Header.SetField(new MsgSeqNum(1));
        m.Header.SetField(new SendingTime(DateTime.UtcNow));
        m.SetField(new Symbol(symbol));
        m.SetField(new NoMDEntries(1));
        var grp = new Group(268, 269);
        grp.SetField(new MDEntryType(type[0]));
        grp.SetField(new MDEntryPx(new decimal(price)));
        grp.SetField(new MDEntrySize(new decimal(qty)));
        m.AddGroup(grp);
        return m;
    }

    private static Message NewAdmin(string msgType)
    {
        var m = new Message();
        m.Header.SetField(new MsgType(msgType));
        m.Header.SetField(new SenderCompID("FIX_PRODUCER"));
        m.Header.SetField(new TargetCompID("FIX_CONSUMER"));
        m.Header.SetField(new MsgSeqNum(1));
        m.Header.SetField(new SendingTime(DateTime.UtcNow));
        return m;
    }
}
