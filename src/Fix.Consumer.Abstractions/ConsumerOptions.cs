namespace Fix.Consumer;

/// <summary>Outbound publisher kinds. Multiple may be combined.</summary>
public enum PublisherKind
{
    SignalR,
    Grpc,
    Redis,
}

/// <summary>QuickFIX/n initiator settings.</summary>
public sealed record QuickFixOptions
{
    /// <summary>Path to a quickfix.cfg file. If null, an in-memory default config is built from the other fields.</summary>
    public string? ConfigPath { get; init; }
    public string SenderCompId { get; init; } = "FIX_CONSUMER";
    public string TargetCompId { get; init; } = "FIX_PRODUCER";
    public string SocketConnectHost { get; init; } = "127.0.0.1";
    public int SocketConnectPort { get; init; } = 5012;
    public int HeartBtInt { get; init; } = 30;
    /// <summary>Directory used by FileStore/FileLog. Defaults to a process-local temp dir.</summary>
    public string? StoreDir { get; init; }
}

/// <summary>Redis publisher settings.</summary>
public sealed record RedisPublisherOptions
{
    /// <summary>StackExchange.Redis configuration string (e.g. "localhost:6379").</summary>
    public string Configuration { get; init; } = "localhost:6379";
    /// <summary>Channel-name prefix; events are published to "{Prefix}.{subscriberId}".</summary>
    public string ChannelPrefix { get; init; } = "fix.events";
}

/// <summary>Consumer-side configuration. Records — cold path.</summary>
public sealed record ConsumerOptions
{
    public const string SectionName = "Consumer";
    public string ProducerHost { get; init; } = "127.0.0.1";
    public int ProducerTcpPort { get; init; } = 5010;
    public int ProducerUdpPort { get; init; } = 5011;
    public int ChannelCapacity { get; init; } = 1024;
    public string[] CorsOrigins { get; init; } = ["http://localhost:4200"];

    /// <summary>Which outbound publishers to enable. Defaults to SignalR only.</summary>
    public PublisherKind[] Publishers { get; init; } = [PublisherKind.SignalR];

    /// <summary>QuickFIX/n initiator settings (only used when the QuickFix transport is selected).</summary>
    public QuickFixOptions QuickFix { get; init; } = new();

    /// <summary>Redis settings (only used when the Redis publisher is enabled).</summary>
    public RedisPublisherOptions Redis { get; init; } = new();
}
