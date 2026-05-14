using System.Buffers;
using System.Buffers.Text;
using System.Runtime.CompilerServices;

namespace Fix.Protocol;

/// <summary>
/// Allocation-free FIX 4.4 encoder/decoder for the small subset of messages this study uses
/// (Logon, Heartbeat, MarketDataIncrementalRefresh).
/// </summary>
/// <remarks>
/// All operations work on <see cref="Span{T}"/> / <see cref="ReadOnlySpan{T}"/> using
/// <see cref="Utf8Parser"/> / <see cref="Utf8Formatter"/> — no string allocations on the hot path.
/// </remarks>
public static class FixInterpreter
{
    private static ReadOnlySpan<byte> Header => "8=FIX.4.4\u0001"u8;

    /// <summary>
    /// Writes a MarketDataIncrementalRefresh (msg type 'X') for a single tick into <paramref name="destination"/>.
    /// </summary>
    /// <returns>true if the buffer was large enough; the encoded length is returned via <paramref name="written"/>.</returns>
    public static bool TryWriteIncrementalRefresh(in MarketTick tick, Span<byte> destination, out int written)
    {
        written = 0;

        // Build the body first (everything between BodyLength and CheckSum) into a stack buffer
        // so we can compute the body length, then assemble the final message.
        Span<byte> body = stackalloc byte[256];
        int bodyLen = 0;

        // 35=X<SOH>
        if (!TryWriteField(body, ref bodyLen, FixTag.MsgType, FixConstants.MsgTypeMarketDataIncrementalRefresh)) return false;
        // 52=<sendingTimeTicks><SOH>  (we ship raw ticks as a long; this is a study, not a session-layer-compliant impl)
        if (!TryWriteField(body, ref bodyLen, FixTag.SendingTime, tick.TimestampTicks)) return false;
        // 55=<symbol><SOH>
        if (!TryWriteField(body, ref bodyLen, FixTag.Symbol, tick.SymbolBytes)) return false;
        // 269=<entryType><SOH>
        if (!TryWriteField(body, ref bodyLen, FixTag.MDEntryType, tick.EntryType)) return false;
        // 270=<price><SOH>
        if (!TryWriteField(body, ref bodyLen, FixTag.MDEntryPx, tick.Price)) return false;
        // 271=<qty><SOH>
        if (!TryWriteField(body, ref bodyLen, FixTag.MDEntrySize, tick.Quantity)) return false;

        // Assemble: 8=FIX.4.4<SOH>9=<bodyLen><SOH><body>10=<chk><SOH>
        var pos = 0;
        if (destination.Length < Header.Length) return false;
        Header.CopyTo(destination);
        pos += Header.Length;

        if (!TryWriteField(destination, ref pos, FixTag.BodyLength, (long)bodyLen)) return false;

        if (destination.Length - pos < bodyLen) return false;
        body[..bodyLen].CopyTo(destination[pos..]);
        pos += bodyLen;

        // Checksum is computed over everything up to (but not including) the 10= field.
        var checksum = ComputeChecksum(destination[..pos]);

        // 10=NNN<SOH>  (always 3 digits, zero-padded)
        if (destination.Length - pos < 7) return false;
        destination[pos++] = (byte)'1';
        destination[pos++] = (byte)'0';
        destination[pos++] = FixConstants.Equal;
        destination[pos++] = (byte)('0' + (checksum / 100));
        destination[pos++] = (byte)('0' + ((checksum / 10) % 10));
        destination[pos++] = (byte)('0' + (checksum % 10));
        destination[pos++] = FixConstants.Soh;

        written = pos;
        return true;
    }

    /// <summary>Writes a Logon ('A') message used by UDP clients to subscribe to a symbol.</summary>
    public static bool TryWriteLogon(ReadOnlySpan<byte> symbol, Span<byte> destination, out int written)
    {
        written = 0;
        Span<byte> body = stackalloc byte[64];
        int bodyLen = 0;
        if (!TryWriteField(body, ref bodyLen, FixTag.MsgType, FixConstants.MsgTypeLogon)) return false;
        if (!TryWriteField(body, ref bodyLen, FixTag.Symbol, symbol)) return false;

        var pos = 0;
        if (destination.Length < Header.Length) return false;
        Header.CopyTo(destination);
        pos += Header.Length;

        if (!TryWriteField(destination, ref pos, FixTag.BodyLength, (long)bodyLen)) return false;
        if (destination.Length - pos < bodyLen) return false;
        body[..bodyLen].CopyTo(destination[pos..]);
        pos += bodyLen;

        var checksum = ComputeChecksum(destination[..pos]);
        if (destination.Length - pos < 7) return false;
        destination[pos++] = (byte)'1';
        destination[pos++] = (byte)'0';
        destination[pos++] = FixConstants.Equal;
        destination[pos++] = (byte)('0' + (checksum / 100));
        destination[pos++] = (byte)('0' + ((checksum / 10) % 10));
        destination[pos++] = (byte)('0' + (checksum % 10));
        destination[pos++] = FixConstants.Soh;
        written = pos;
        return true;
    }

    /// <summary>
    /// Attempts to parse a single, complete FIX MarketDataIncrementalRefresh message into a
    /// <see cref="MarketTick"/>. Validates only that required tags are present; this is a study impl.
    /// </summary>
    public static bool TryParseIncrementalRefresh(ReadOnlySpan<byte> message, out MarketTick tick)
    {
        tick = default;
        ReadOnlySpan<byte> symbol = default;
        double price = 0, qty = 0;
        long ts = 0;
        byte entryType = 0;
        byte msgType = 0;
        bool haveSymbol = false, havePrice = false, haveQty = false, haveType = false, haveTs = false;

        var rem = message;
        while (rem.Length > 0)
        {
            var soh = rem.IndexOf(FixConstants.Soh);
            if (soh < 0) break;
            var field = rem[..soh];
            rem = rem[(soh + 1)..];

            var eq = field.IndexOf(FixConstants.Equal);
            if (eq < 0) continue;
            if (!Utf8Parser.TryParse(field[..eq], out int tag, out _)) continue;
            var value = field[(eq + 1)..];

            switch ((FixTag)tag)
            {
                case FixTag.MsgType:
                    if (value.Length == 1) msgType = value[0];
                    break;
                case FixTag.Symbol:
                    symbol = value;
                    haveSymbol = true;
                    break;
                case FixTag.MDEntryType:
                    if (value.Length == 1) { entryType = value[0]; haveType = true; }
                    break;
                case FixTag.MDEntryPx:
                    if (Utf8Parser.TryParse(value, out price, out _)) havePrice = true;
                    break;
                case FixTag.MDEntrySize:
                    if (Utf8Parser.TryParse(value, out qty, out _)) haveQty = true;
                    break;
                case FixTag.SendingTime:
                    if (Utf8Parser.TryParse(value, out ts, out _)) haveTs = true;
                    break;
            }
        }

        if (msgType != FixConstants.MsgTypeMarketDataIncrementalRefresh) return false;
        if (!(haveSymbol && havePrice && haveQty && haveType && haveTs)) return false;

        tick = new MarketTick(symbol, price, qty, ts, entryType);
        return true;
    }

    /// <summary>Attempts to read the message type ('35=') from a parsed FIX message.</summary>
    public static bool TryGetMsgType(ReadOnlySpan<byte> message, out byte msgType)
    {
        msgType = 0;
        var rem = message;
        while (rem.Length > 0)
        {
            var soh = rem.IndexOf(FixConstants.Soh);
            if (soh < 0) break;
            var field = rem[..soh];
            rem = rem[(soh + 1)..];
            var eq = field.IndexOf(FixConstants.Equal);
            if (eq < 0) continue;
            if (!Utf8Parser.TryParse(field[..eq], out int tag, out _)) continue;
            if (tag == (int)FixTag.MsgType && field.Length - eq - 1 == 1)
            {
                msgType = field[eq + 1];
                return true;
            }
        }
        return false;
    }

    /// <summary>Attempts to extract the Symbol (tag 55) from a parsed FIX message.</summary>
    public static bool TryGetSymbol(ReadOnlySpan<byte> message, Span<byte> symbolDestination, out int symbolLength)
    {
        symbolLength = 0;
        var rem = message;
        while (rem.Length > 0)
        {
            var soh = rem.IndexOf(FixConstants.Soh);
            if (soh < 0) break;
            var field = rem[..soh];
            rem = rem[(soh + 1)..];
            var eq = field.IndexOf(FixConstants.Equal);
            if (eq < 0) continue;
            if (!Utf8Parser.TryParse(field[..eq], out int tag, out _)) continue;
            if (tag == (int)FixTag.Symbol)
            {
                var v = field[(eq + 1)..];
                if (v.Length > symbolDestination.Length) return false;
                v.CopyTo(symbolDestination);
                symbolLength = v.Length;
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Looks for the end of a single FIX message inside <paramref name="buffer"/>. Returns the
    /// length (including trailing SOH) if a complete message ending with the standard
    /// "10=NNN&lt;SOH&gt;" trailer is found, otherwise 0.
    /// </summary>
    public static int TryFindMessageBoundary(ReadOnlySpan<byte> buffer)
    {
        // Look for SOH '1' '0' '=' d d d SOH pattern.
        for (int i = 0; i + 7 < buffer.Length; i++)
        {
            if (buffer[i] == FixConstants.Soh
                && buffer[i + 1] == (byte)'1'
                && buffer[i + 2] == (byte)'0'
                && buffer[i + 3] == FixConstants.Equal
                && IsDigit(buffer[i + 4]) && IsDigit(buffer[i + 5]) && IsDigit(buffer[i + 6])
                && buffer[i + 7] == FixConstants.Soh)
            {
                return i + 8; // total length of the message
            }
        }
        return 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDigit(byte b) => b >= (byte)'0' && b <= (byte)'9';

    public static byte ComputeChecksum(ReadOnlySpan<byte> data)
    {
        uint sum = 0;
        for (int i = 0; i < data.Length; i++) sum += data[i];
        return (byte)(sum % 256);
    }

    private static bool TryWriteField(Span<byte> dest, ref int pos, FixTag tag, byte value)
    {
        if (!TryWriteTagPrefix(dest, ref pos, tag)) return false;
        if (dest.Length - pos < 2) return false;
        dest[pos++] = value;
        dest[pos++] = FixConstants.Soh;
        return true;
    }

    private static bool TryWriteField(Span<byte> dest, ref int pos, FixTag tag, ReadOnlySpan<byte> value)
    {
        if (!TryWriteTagPrefix(dest, ref pos, tag)) return false;
        if (dest.Length - pos < value.Length + 1) return false;
        value.CopyTo(dest[pos..]);
        pos += value.Length;
        dest[pos++] = FixConstants.Soh;
        return true;
    }

    private static bool TryWriteField(Span<byte> dest, ref int pos, FixTag tag, long value)
    {
        if (!TryWriteTagPrefix(dest, ref pos, tag)) return false;
        if (!Utf8Formatter.TryFormat(value, dest[pos..], out int w)) return false;
        pos += w;
        if (dest.Length - pos < 1) return false;
        dest[pos++] = FixConstants.Soh;
        return true;
    }

    private static bool TryWriteField(Span<byte> dest, ref int pos, FixTag tag, double value)
    {
        if (!TryWriteTagPrefix(dest, ref pos, tag)) return false;
        if (!Utf8Formatter.TryFormat(value, dest[pos..], out int w, new StandardFormat('F', 5))) return false;
        pos += w;
        if (dest.Length - pos < 1) return false;
        dest[pos++] = FixConstants.Soh;
        return true;
    }

    private static bool TryWriteTagPrefix(Span<byte> dest, ref int pos, FixTag tag)
    {
        if (!Utf8Formatter.TryFormat((int)tag, dest[pos..], out int w)) return false;
        pos += w;
        if (dest.Length - pos < 1) return false;
        dest[pos++] = FixConstants.Equal;
        return true;
    }
}
