using System.Collections.Generic;

namespace Fix.Protocol;

/// <summary>
/// Discriminator for the FIX events surfaced to subscribers. The numeric value of each
/// member intentionally matches the FIX <c>35=</c> message-type byte where it is a single
/// printable ASCII character, but consumers should treat the enum as opaque (the wire
/// representation is the <c>35=</c> tag value, not this integer).
/// </summary>
public enum FixEventKind
{
    /// <summary>Unknown / unmapped message type.</summary>
    Unknown = 0,

    // ---- Session-layer events (admin) ----
    Heartbeat,                       // 35=0
    TestRequest,                     // 35=1
    ResendRequest,                   // 35=2
    Reject,                          // 35=3
    SequenceReset,                   // 35=4
    Logout,                          // 35=5
    Logon,                           // 35=A

    // ---- Application events ----
    News,                            // 35=B
    MarketDataSnapshotFullRefresh,   // 35=W
    MarketDataIncrementalRefresh,    // 35=X
    MarketDataRequestReject,         // 35=Y
    BusinessMessageReject,           // 35=j
}

/// <summary>
/// Marker for FIX event payloads. Implementations are immutable cold-path records (or the
/// blittable <see cref="MarketTick"/> struct boxed once at the publisher boundary).
/// </summary>
public interface IFixEventPayload { }

/// <summary>
/// Cold-path envelope for a single FIX event. The hot path keeps using <see cref="MarketTick"/>
/// internally; this record is materialised once when an event leaves a connection's channel and
/// crosses an <c>IEventPublisher</c> boundary (SignalR / gRPC / Redis / …).
/// </summary>
/// <param name="Kind">Event discriminator. See <see cref="FixEventKind"/>.</param>
/// <param name="Symbol">Symbol the event is about, if applicable (e.g. for market-data).</param>
/// <param name="SenderCompId">FIX <c>49=</c> tag, if known.</param>
/// <param name="TargetCompId">FIX <c>56=</c> tag, if known.</param>
/// <param name="MsgSeqNum">FIX <c>34=</c> tag, if known.</param>
/// <param name="SendingTimeTicks">FIX <c>52=</c> sending time as .NET ticks (UTC), if known.</param>
/// <param name="Source">Free-form transport tag — "TCP", "UDP", "QuickFIX". Useful for UI / logs.</param>
/// <param name="Payload">Optional kind-specific payload. May be <c>null</c>.</param>
public sealed record FixEvent(
    FixEventKind Kind,
    string? Symbol,
    string? SenderCompId,
    string? TargetCompId,
    long? MsgSeqNum,
    long? SendingTimeTicks,
    string Source,
    IFixEventPayload? Payload);

/// <summary>Payload for market-data events (35=X / 35=W).</summary>
public sealed record MarketDataPayload(double Price, double Quantity, string EntryType) : IFixEventPayload;

/// <summary>Payload for session-layer reject events (35=3 / 35=j / 35=Y).</summary>
public sealed record RejectPayload(int? RefSeqNum, string? RefMsgType, string? Reason) : IFixEventPayload;

/// <summary>Generic key/value payload for events without a richer model.</summary>
public sealed record FieldsPayload(IReadOnlyDictionary<int, string> Fields) : IFixEventPayload;

/// <summary>Helpers for translating between the <c>35=</c> byte and <see cref="FixEventKind"/>.</summary>
public static class FixEventKinds
{
    /// <summary>Maps a FIX <c>35=</c> message-type string to a <see cref="FixEventKind"/>.</summary>
    public static FixEventKind FromMsgType(string msgType) => msgType switch
    {
        "0" => FixEventKind.Heartbeat,
        "1" => FixEventKind.TestRequest,
        "2" => FixEventKind.ResendRequest,
        "3" => FixEventKind.Reject,
        "4" => FixEventKind.SequenceReset,
        "5" => FixEventKind.Logout,
        "A" => FixEventKind.Logon,
        "B" => FixEventKind.News,
        "W" => FixEventKind.MarketDataSnapshotFullRefresh,
        "X" => FixEventKind.MarketDataIncrementalRefresh,
        "Y" => FixEventKind.MarketDataRequestReject,
        "j" => FixEventKind.BusinessMessageReject,
        _ => FixEventKind.Unknown,
    };

    /// <summary>Maps a single-byte FIX <c>35=</c> message type to a <see cref="FixEventKind"/>.</summary>
    public static FixEventKind FromMsgType(byte msgType) => msgType switch
    {
        (byte)'0' => FixEventKind.Heartbeat,
        (byte)'1' => FixEventKind.TestRequest,
        (byte)'2' => FixEventKind.ResendRequest,
        (byte)'3' => FixEventKind.Reject,
        (byte)'4' => FixEventKind.SequenceReset,
        (byte)'5' => FixEventKind.Logout,
        (byte)'A' => FixEventKind.Logon,
        (byte)'B' => FixEventKind.News,
        (byte)'W' => FixEventKind.MarketDataSnapshotFullRefresh,
        (byte)'X' => FixEventKind.MarketDataIncrementalRefresh,
        (byte)'Y' => FixEventKind.MarketDataRequestReject,
        (byte)'j' => FixEventKind.BusinessMessageReject,
        _ => FixEventKind.Unknown,
    };

    /// <summary>All event kinds the consumer may surface (excludes <see cref="FixEventKind.Unknown"/>).</summary>
    public static IReadOnlyList<FixEventKind> All { get; } = new[]
    {
        FixEventKind.Heartbeat,
        FixEventKind.TestRequest,
        FixEventKind.ResendRequest,
        FixEventKind.Reject,
        FixEventKind.SequenceReset,
        FixEventKind.Logout,
        FixEventKind.Logon,
        FixEventKind.News,
        FixEventKind.MarketDataSnapshotFullRefresh,
        FixEventKind.MarketDataIncrementalRefresh,
        FixEventKind.MarketDataRequestReject,
        FixEventKind.BusinessMessageReject,
    };
}
