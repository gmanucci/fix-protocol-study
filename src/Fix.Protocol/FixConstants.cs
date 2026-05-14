namespace Fix.Protocol;

/// <summary>
/// FIX protocol constants. Tag numbers follow the FIX 4.4 specification.
/// </summary>
public static class FixConstants
{
    /// <summary>SOH delimiter (0x01) used to separate FIX fields.</summary>
    public const byte Soh = 0x01;

    /// <summary>'=' separator (0x3D) between tag and value.</summary>
    public const byte Equal = (byte)'=';

    public static ReadOnlySpan<byte> BeginStringFix44 => "FIX.4.4"u8;

    // Message types (tag 35)
    public const byte MsgTypeHeartbeat = (byte)'0';
    public const byte MsgTypeLogon = (byte)'A';
    public const byte MsgTypeMarketDataSnapshot = (byte)'W';
    public const byte MsgTypeMarketDataIncrementalRefresh = (byte)'X';
}

/// <summary>
/// Standard FIX 4.4 tag numbers used by this project.
/// </summary>
public enum FixTag
{
    BeginString = 8,
    BodyLength = 9,
    MsgType = 35,
    SenderCompID = 49,
    TargetCompID = 56,
    MsgSeqNum = 34,
    SendingTime = 52,
    Symbol = 55,
    MDEntryType = 269,
    MDEntryPx = 270,
    MDEntrySize = 271,
    MDUpdateAction = 279,
    Side = 54,
    CheckSum = 10,
}

/// <summary>MDEntryType (tag 269) values.</summary>
public static class MdEntryType
{
    public const byte Bid = (byte)'0';
    public const byte Offer = (byte)'1';
    public const byte Trade = (byte)'2';
}
