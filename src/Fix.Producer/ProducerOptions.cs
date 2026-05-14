namespace Fix.Producer;

/// <summary>
/// Strongly-typed configuration. Records are fine here — bound once at startup, cold path.
/// </summary>
public sealed record ProducerOptions
{
    public const string SectionName = "Producer";

    /// <summary>Markets (symbols) to simulate.</summary>
    public string[] Markets { get; init; } = ["EURUSD", "GBPUSD", "USDJPY", "AAPL", "MSFT"];

    /// <summary>How often each market emits a tick. Lower = faster. Use 0 for unthrottled.</summary>
    public int TickIntervalMicroseconds { get; init; } = 1_000;

    /// <summary>Bounded channel capacity per market (drop-oldest on overflow to keep latency low).</summary>
    public int ChannelCapacity { get; init; } = 1024;

    /// <summary>TCP listener.</summary>
    public TransportOptions Tcp { get; init; } = new() { Enabled = true, Port = 5010 };

    /// <summary>UDP listener.</summary>
    public TransportOptions Udp { get; init; } = new() { Enabled = true, Port = 5011 };
}

public sealed record TransportOptions
{
    public bool Enabled { get; init; } = true;
    public int Port { get; init; }
}
