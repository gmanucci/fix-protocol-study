namespace Fix.Consumer;

/// <summary>Consumer-side configuration. Records — cold path.</summary>
public sealed record ConsumerOptions
{
    public const string SectionName = "Consumer";
    public string ProducerHost { get; init; } = "127.0.0.1";
    public int ProducerTcpPort { get; init; } = 5010;
    public int ProducerUdpPort { get; init; } = 5011;
    public int ChannelCapacity { get; init; } = 1024;
    public string[] CorsOrigins { get; init; } = ["http://localhost:4200"];
}
