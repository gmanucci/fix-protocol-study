using System.Collections.Concurrent;
using System.Threading.Channels;
using Fix.Consumer;
using Fix.Protocol;
using Microsoft.Extensions.Logging;
using QuickFix;
using QuickFix.Fields;
using FixMessage = QuickFix.Message;
using MarketDataRequest = QuickFix.FIX44.MarketDataRequest;

namespace Fix.Consumer.QuickFix;

/// <summary>
/// QuickFIX/n <see cref="IApplication"/> that translates every received FIX message into a
/// <see cref="FixEvent"/> and fans it out to all channels subscribed to that symbol (or to
/// every wildcard subscriber, for session-layer events that have no symbol).
/// </summary>
/// <remarks>
/// One application instance is shared by the whole consumer process. <see cref="Register"/> /
/// <see cref="Unregister"/> are thread-safe and add/remove subscribers without restarting the
/// underlying QuickFIX session.
///
/// On <see cref="OnLogon"/>, every active per-symbol subscriber receives a
/// <c>MarketDataRequest (35=V)</c> — exactly the flow described in the plan.
/// </remarks>
public sealed class QuickFixApplication : IApplication
{
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, Subscriber> _subscribers = new();
    private SessionID? _activeSession;

    public QuickFixApplication(ILogger logger) => _logger = logger;

    /// <summary>Adds a subscriber; events for <paramref name="symbol"/> (or any if "*") are forwarded to <paramref name="writer"/>.</summary>
    public void Register(string subscriberKey, string symbol, IReadOnlySet<FixEventKind> kinds, ChannelWriter<FixEvent> writer)
    {
        _subscribers[subscriberKey] = new Subscriber(symbol, kinds, writer);
        // If we already have an active session and this is a per-symbol subscriber, send a MarketDataRequest now.
        if (_activeSession is { } s && symbol != "*")
        {
            try { SendMarketDataRequest(s, symbol); }
            catch (Exception ex) { _logger.LogWarning(ex, "MarketDataRequest for {Symbol} failed", symbol); }
        }
    }

    public void Unregister(string subscriberKey)
    {
        if (_subscribers.TryRemove(subscriberKey, out var sub))
            sub.Writer.TryComplete();
    }

    // ---- IApplication ----

    public void OnCreate(SessionID sessionID) { }

    public void OnLogon(SessionID sessionID)
    {
        _activeSession = sessionID;
        _logger.LogInformation("QuickFIX session logged on: {Session}", sessionID);
        // Send a MarketDataRequest for every per-symbol subscriber known so far.
        foreach (var sub in _subscribers.Values)
        {
            if (sub.Symbol == "*") continue;
            try { SendMarketDataRequest(sessionID, sub.Symbol); }
            catch (Exception ex) { _logger.LogWarning(ex, "MarketDataRequest for {Symbol} failed", sub.Symbol); }
        }
    }

    public void OnLogout(SessionID sessionID)
    {
        _activeSession = null;
        _logger.LogInformation("QuickFIX session logged out: {Session}", sessionID);
    }

    public void ToAdmin(FixMessage message, SessionID sessionID) { }
    public void ToApp(FixMessage message, SessionID sessionId) { }

    public void FromAdmin(FixMessage message, SessionID sessionID) => Dispatch(message);
    public void FromApp(FixMessage message, SessionID sessionID) => Dispatch(message);

    // ---- internals ----

    /// <summary>Test seam: directly inject a <see cref="FixMessage"/> without a real session.</summary>
    public void Dispatch(FixMessage message)
    {
        FixEvent? evt;
        try { evt = ToFixEvent(message); }
        catch (Exception ex) { _logger.LogWarning(ex, "Failed to map QuickFIX message"); return; }
        if (evt is null) return;

        foreach (var sub in _subscribers.Values)
        {
            if (!sub.Kinds.Contains(evt.Kind)) continue;
            // Symbol-targeted subscribers only receive events for matching symbols, or events without a symbol.
            if (sub.Symbol != "*" && evt.Symbol is not null && !string.Equals(sub.Symbol, evt.Symbol, StringComparison.Ordinal))
                continue;
            sub.Writer.TryWrite(evt);
        }
    }

    private static FixEvent? ToFixEvent(FixMessage message)
    {
        var header = message.Header;
        var msgType = header.IsSetField(Tags.MsgType) ? header.GetString(Tags.MsgType) : string.Empty;
        var kind = FixEventKinds.FromMsgType(msgType);
        if (kind == FixEventKind.Unknown) return null;

        string? sender = header.IsSetField(Tags.SenderCompID) ? header.GetString(Tags.SenderCompID) : null;
        string? target = header.IsSetField(Tags.TargetCompID) ? header.GetString(Tags.TargetCompID) : null;
        long? seq = header.IsSetField(Tags.MsgSeqNum) ? header.GetInt(Tags.MsgSeqNum) : (long?)null;
        long? sendingTimeTicks = null;
        if (header.IsSetField(Tags.SendingTime))
        {
            try { sendingTimeTicks = header.GetDateTime(Tags.SendingTime).Ticks; }
            catch { /* best effort */ }
        }

        string? symbol = message.IsSetField(Tags.Symbol) ? message.GetString(Tags.Symbol) : null;
        IFixEventPayload? payload = null;

        switch (kind)
        {
            case FixEventKind.MarketDataIncrementalRefresh:
            case FixEventKind.MarketDataSnapshotFullRefresh:
                payload = TryReadMarketData(message);
                break;
            case FixEventKind.Reject:
            case FixEventKind.BusinessMessageReject:
            case FixEventKind.MarketDataRequestReject:
                payload = ReadReject(message);
                break;
        }

        return new FixEvent(kind, symbol, sender, target, seq, sendingTimeTicks, "QuickFIX", payload);
    }

    private static IFixEventPayload? TryReadMarketData(FixMessage message)
    {
        // Look at the first MDEntry group (study impl); ignore multi-entry messages beyond the first.
        try
        {
            // Tag 268 = NoMDEntries
            if (!message.IsSetField(268)) return null;
            // Use a generic group of MDEntries (Tag 269 leads each entry).
            var grp = new Group(268, 269);
            message.GetGroup(1, grp);
            double price = grp.IsSetField(Tags.MDEntryPx) ? (double)grp.GetDecimal(Tags.MDEntryPx) : 0.0;
            double qty = grp.IsSetField(Tags.MDEntrySize) ? (double)grp.GetDecimal(Tags.MDEntrySize) : 0.0;
            string entry = grp.IsSetField(Tags.MDEntryType) ? grp.GetString(Tags.MDEntryType) switch
            {
                "0" => "Bid",
                "1" => "Offer",
                "2" => "Trade",
                _ => "Unknown",
            } : "Unknown";
            return new MarketDataPayload(price, qty, entry);
        }
        catch
        {
            return null;
        }
    }

    private static RejectPayload ReadReject(FixMessage message)
    {
        int? refSeq = message.IsSetField(Tags.RefSeqNum) ? message.GetInt(Tags.RefSeqNum) : null;
        string? refMsgType = message.IsSetField(Tags.RefMsgType) ? message.GetString(Tags.RefMsgType) : null;
        string? text = message.IsSetField(Tags.Text) ? message.GetString(Tags.Text) : null;
        return new RejectPayload(refSeq, refMsgType, text);
    }

    private static void SendMarketDataRequest(SessionID sessionID, string symbol)
    {
        var req = new MarketDataRequest(
            new MDReqID(Guid.NewGuid().ToString("N")),
            new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_PLUS_UPDATES),
            new MarketDepth(0));

        var entryTypes = new MarketDataRequest.NoMDEntryTypesGroup();
        entryTypes.MDEntryType = new MDEntryType(MDEntryType.BID); req.AddGroup(entryTypes);
        entryTypes.MDEntryType = new MDEntryType(MDEntryType.OFFER); req.AddGroup(entryTypes);
        entryTypes.MDEntryType = new MDEntryType(MDEntryType.TRADE); req.AddGroup(entryTypes);

        var sym = new MarketDataRequest.NoRelatedSymGroup { Symbol = new Symbol(symbol) };
        req.AddGroup(sym);

        Session.SendToTarget(req, sessionID);
    }

    private static class Tags
    {
        public const int MsgType = 35;
        public const int SenderCompID = 49;
        public const int TargetCompID = 56;
        public const int MsgSeqNum = 34;
        public const int SendingTime = 52;
        public const int Symbol = 55;
        public const int MDEntryPx = 270;
        public const int MDEntrySize = 271;
        public const int MDEntryType = 269;
        public const int RefSeqNum = 45;
        public const int RefMsgType = 372;
        public const int Text = 58;
    }

    private sealed record Subscriber(string Symbol, IReadOnlySet<FixEventKind> Kinds, ChannelWriter<FixEvent> Writer);
}
