using System.Net.Sockets;
using System.Text;
using Fix.Producer;
using Fix.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Fix.Producer.Tests;

public class MarketDataSourceTests
{
    [Fact]
    public async Task Produces_ticks_for_configured_symbols()
    {
        var opts = Options.Create(new ProducerOptions
        {
            Markets = ["AAA", "BBB"],
            TickIntervalMicroseconds = 0,
            ChannelCapacity = 16,
        });
        var src = new MarketDataSource(opts, NullLogger<MarketDataSource>.Instance);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var run = ((Microsoft.Extensions.Hosting.IHostedService)src).StartAsync(cts.Token);

        var reader = src.GetReader("AAA");
        Assert.NotNull(reader);

        var got = await reader!.ReadAsync(cts.Token);
        Assert.Equal("AAA", got.Symbol);

        cts.Cancel();
        try { await ((Microsoft.Extensions.Hosting.IHostedService)src).StopAsync(CancellationToken.None); } catch { }
    }

    [Fact]
    public void GetReader_byBytes_matchesStringLookup()
    {
        var opts = Options.Create(new ProducerOptions { Markets = ["EURUSD"], TickIntervalMicroseconds = 1000 });
        var src = new MarketDataSource(opts, NullLogger<MarketDataSource>.Instance);
        Assert.NotNull(src.GetReader("EURUSD"u8));
        Assert.Null(src.GetReader("MISSING"u8));
    }
}

public class TcpFixTransportTests
{
    [Fact]
    public async Task Tcp_subscriber_receives_well_formed_fix_messages()
    {
        var port = GetFreeTcpPort();
        var opts = Options.Create(new ProducerOptions
        {
            Markets = ["EURUSD"],
            TickIntervalMicroseconds = 0,
            ChannelCapacity = 64,
            Tcp = new() { Enabled = true, Port = port },
            Udp = new() { Enabled = false, Port = 0 },
        });
        var src = new MarketDataSource(opts, NullLogger<MarketDataSource>.Instance);
        var tcp = new TcpFixTransport(src, opts, NullLogger<TcpFixTransport>.Instance);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ((Microsoft.Extensions.Hosting.IHostedService)src).StartAsync(cts.Token);
        var serverTask = tcp.RunAsync(cts.Token);
        await Task.Delay(150, cts.Token);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", port, cts.Token);
        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.ASCII.GetBytes("SUB EURUSD\n"), cts.Token);

        var buf = new byte[512];
        int read = 0;
        // Read until we have at least one complete FIX message.
        while (FixInterpreter.TryFindMessageBoundary(buf.AsSpan(0, read)) == 0)
        {
            var n = await stream.ReadAsync(buf.AsMemory(read), cts.Token);
            Assert.True(n > 0, "stream closed before a complete message arrived");
            read += n;
        }

        var len = FixInterpreter.TryFindMessageBoundary(buf.AsSpan(0, read));
        Assert.True(len > 0);
        Assert.True(FixInterpreter.TryParseIncrementalRefresh(buf.AsSpan(0, len), out var tick));
        Assert.Equal("EURUSD", tick.Symbol);

        cts.Cancel();
    }

    private static int GetFreeTcpPort()
    {
        var l = new TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return p;
    }
}
