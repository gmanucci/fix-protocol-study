using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fix.Protocol;

/// <summary>
/// Compact, allocation-free representation of a market data tick that can travel through
/// <see cref="System.Threading.Channels.Channel{T}"/> instances by value.
/// </summary>
/// <remarks>
/// Plain <c>readonly struct</c> with explicit layout is used (instead of <c>record</c> or
/// <c>record struct</c>) because:
/// <list type="bullet">
///   <item>A class-based <c>record</c> heap-allocates per tick, generating GC pressure.</item>
///   <item>A <c>record struct</c> would work but adds compiler-synthesised members
///         (<c>Equals</c>, <c>GetHashCode</c>, <c>ToString</c>) we don't need on the hot path.</item>
///   <item>A <c>readonly struct</c> with <see cref="LayoutKind.Sequential"/> packs into a
///         predictable, cache-friendly footprint, plays well with <see cref="Span{T}"/>
///         interop, and never boxes when written to a <see cref="System.Threading.Channels.Channel{T}"/>.</item>
/// </list>
/// Symbol is encoded as a fixed 8-byte buffer (ASCII, NUL-padded) so the struct is fully
/// blittable and contains no managed references.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Pack = 8)]
public unsafe struct MarketTick : IEquatable<MarketTick>
{
    public const int MaxSymbolLength = 8;

    private fixed byte _symbol[MaxSymbolLength];

    public double Price;
    public double Quantity;
    public long TimestampTicks;
    public byte EntryType; // see MdEntryType
    public byte SymbolLength;

    public MarketTick(ReadOnlySpan<byte> symbol, double price, double quantity, long timestampTicks, byte entryType)
    {
        var len = symbol.Length;
        if (len > MaxSymbolLength) len = MaxSymbolLength;
        fixed (byte* dst = _symbol)
        {
            for (int i = 0; i < len; i++) dst[i] = symbol[i];
            for (int i = len; i < MaxSymbolLength; i++) dst[i] = 0;
        }
        SymbolLength = (byte)len;
        Price = price;
        Quantity = quantity;
        TimestampTicks = timestampTicks;
        EntryType = entryType;
    }

    public ReadOnlySpan<byte> SymbolBytes
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            fixed (byte* p = _symbol)
            {
                return new ReadOnlySpan<byte>(p, SymbolLength);
            }
        }
    }

    public string Symbol => System.Text.Encoding.ASCII.GetString(SymbolBytes);

    public bool Equals(MarketTick other) =>
        Price == other.Price &&
        Quantity == other.Quantity &&
        TimestampTicks == other.TimestampTicks &&
        EntryType == other.EntryType &&
        SymbolBytes.SequenceEqual(other.SymbolBytes);

    public override bool Equals(object? obj) => obj is MarketTick t && Equals(t);

    public override int GetHashCode() => HashCode.Combine(Price, Quantity, TimestampTicks, EntryType, SymbolLength);
}
