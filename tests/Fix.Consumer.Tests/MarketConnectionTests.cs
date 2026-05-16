using Fix.Consumer;
using Fix.Producer;
using Fix.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Fix.Consumer.Tests;

public class TcpMarketConnectionTests
{
    [Fact]
    public async Task End_to_end_TCP_subscriber_receives_parsed_ticks()
    {
        var port = GetFreeTcpPort();
        var producerOpts = Options.Create(new ProducerOptions
        {
            Markets = ["EURUSD"],
            TickIntervalMicroseconds = 0,
            ChannelCapacity = 64,
            Tcp = new() { Enabled = true, Port = port },
            Udp = new() { Enabled = false, Port = 0 },
        });
        var src = new MarketDataSource(producerOpts, NullLogger<MarketDataSource>.Instance);
        var tcpServer = new TcpFixTransport(src, producerOpts, NullLogger<TcpFixTransport>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await ((Microsoft.Extensions.Hosting.IHostedService)src).StartAsync(cts.Token);
        var serverTask = tcpServer.RunAsync(cts.Token);
        await Task.Delay(150, cts.Token);

        var consumerOpts = new ConsumerOptions
        {
            ProducerHost = "127.0.0.1",
            ProducerTcpPort = port,
            ChannelCapacity = 64,
        };
        await using var conn = new TcpMarketConnection("EURUSD", consumerOpts, NullLogger<TcpMarketConnection>.Instance);
        await conn.StartAsync(new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh }, cts.Token);

        var evt = await conn.Events.ReadAsync(cts.Token);
        Assert.Equal(FixEventKind.MarketDataIncrementalRefresh, evt.Kind);
        Assert.Equal("EURUSD", evt.Symbol);
        var md = Assert.IsType<MarketDataPayload>(evt.Payload);
        Assert.True(md.Price > 0);

        cts.Cancel();
    }

    private static int GetFreeTcpPort()
    {
        var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return p;
    }
}

public class UdpMarketConnectionTests
{
    [Fact]
    public async Task End_to_end_UDP_subscriber_receives_parsed_ticks()
    {
        var port = GetFreeUdpPort();
        var producerOpts = Options.Create(new ProducerOptions
        {
            Markets = ["AAPL"],
            TickIntervalMicroseconds = 1_000,
            ChannelCapacity = 64,
            Tcp = new() { Enabled = false, Port = 0 },
            Udp = new() { Enabled = true, Port = port },
        });
        var src = new MarketDataSource(producerOpts, NullLogger<MarketDataSource>.Instance);
        var udpServer = new UdpFixTransport(src, producerOpts, NullLogger<UdpFixTransport>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ((Microsoft.Extensions.Hosting.IHostedService)src).StartAsync(cts.Token);
        var serverTask = udpServer.RunAsync(cts.Token);
        await Task.Delay(150, cts.Token);

        var consumerOpts = new ConsumerOptions
        {
            ProducerHost = "127.0.0.1",
            ProducerUdpPort = port,
            ChannelCapacity = 64,
        };
        await using var conn = new UdpMarketConnection("AAPL", consumerOpts, NullLogger<UdpMarketConnection>.Instance);
        await conn.StartAsync(new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh }, cts.Token);

        var evt = await conn.Events.ReadAsync(cts.Token);
        Assert.Equal("AAPL", evt.Symbol);
        var md = Assert.IsType<MarketDataPayload>(evt.Payload);
        Assert.True(md.Price > 0);

        cts.Cancel();
    }

    private static int GetFreeUdpPort()
    {
        using var s = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Dgram, System.Net.Sockets.ProtocolType.Udp);
        s.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        return ((System.Net.IPEndPoint)s.LocalEndPoint!).Port;
    }
}
