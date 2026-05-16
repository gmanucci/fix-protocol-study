using System.Threading.Channels;
using Fix.Consumer;
using Fix.Protocol;
using Microsoft.Extensions.Logging.Abstractions;

namespace Fix.Consumer.Tests;

public class PublisherTests
{
    [Fact]
    public async Task CompositeEventPublisher_fans_out_to_every_inner()
    {
        var a = new RecordingPublisher();
        var b = new RecordingPublisher();
        var composite = new CompositeEventPublisher(new IEventPublisher[] { a, b });

        var evt = new FixEvent(FixEventKind.Heartbeat, null, null, null, null, null, "Test", null);
        await composite.PublishAsync("sub", evt, CancellationToken.None);
        await composite.CompleteAsync("sub", CancellationToken.None);

        Assert.Single(a.Published);
        Assert.Single(b.Published);
        Assert.Equal(1, a.Completed);
        Assert.Equal(1, b.Completed);
    }

    [Fact]
    public async Task CompositeEventPublisher_swallows_individual_failures()
    {
        var failing = new ThrowingPublisher();
        var ok = new RecordingPublisher();
        var composite = new CompositeEventPublisher(new IEventPublisher[] { failing, ok });

        var evt = new FixEvent(FixEventKind.Heartbeat, null, null, null, null, null, "Test", null);
        await composite.PublishAsync("sub", evt, CancellationToken.None);

        Assert.Single(ok.Published);
    }

    [Fact]
    public async Task SubscriptionPump_forwards_events_until_channel_completes()
    {
        var conn = new FakeConnection("EURUSD");
        var pub = new RecordingPublisher();
        await using var pump = new SubscriptionPump(conn, pub, "sub-1", NullLogger.Instance, CancellationToken.None);
        await pump.StartAsync(new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh });

        conn.Push(new FixEvent(FixEventKind.MarketDataIncrementalRefresh, "EURUSD", null, null, null, null, "TCP", null));
        conn.Push(new FixEvent(FixEventKind.MarketDataIncrementalRefresh, "EURUSD", null, null, null, null, "TCP", null));
        conn.Complete();

        // Wait briefly for the pump to drain.
        for (int i = 0; i < 50 && pub.Published.Count < 2; i++) await Task.Delay(20);
        Assert.Equal(2, pub.Published.Count);
    }

    [Fact]
    public async Task GrpcEventPublisher_routes_only_to_matching_subscriber()
    {
        var pub = new GrpcEventPublisher();
        var ch = Channel.CreateUnbounded<FixEvent>();
        pub.Register("sub-A", ch);

        var evt = new FixEvent(FixEventKind.Heartbeat, null, null, null, null, null, "TCP", null);
        await pub.PublishAsync("sub-A", evt, CancellationToken.None);
        await pub.PublishAsync("sub-B", evt, CancellationToken.None); // no subscriber → dropped

        Assert.True(ch.Reader.TryRead(out _));
        Assert.False(ch.Reader.TryRead(out _));
    }

    private sealed class RecordingPublisher : IEventPublisher
    {
        public readonly List<FixEvent> Published = new();
        public int Completed;
        public Task PublishAsync(string id, FixEvent evt, CancellationToken ct) { Published.Add(evt); return Task.CompletedTask; }
        public Task CompleteAsync(string id, CancellationToken ct) { Completed++; return Task.CompletedTask; }
    }

    private sealed class ThrowingPublisher : IEventPublisher
    {
        public Task PublishAsync(string id, FixEvent evt, CancellationToken ct) => throw new InvalidOperationException();
        public Task CompleteAsync(string id, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeConnection : IMarketConnection
    {
        private readonly Channel<FixEvent> _ch = Channel.CreateUnbounded<FixEvent>();
        public FakeConnection(string symbol) { Symbol = symbol; }
        public string Symbol { get; }
        public string Transport => "Fake";
        public ChannelReader<FixEvent> Events => _ch.Reader;
        public Task StartAsync(IReadOnlySet<FixEventKind> kinds, CancellationToken ct) => Task.CompletedTask;
        public void Push(FixEvent evt) => _ch.Writer.TryWrite(evt);
        public void Complete() => _ch.Writer.TryComplete();
        public ValueTask DisposeAsync() { _ch.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
}
