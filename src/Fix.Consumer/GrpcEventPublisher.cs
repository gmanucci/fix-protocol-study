using System.Collections.Concurrent;
using System.Threading.Channels;
using Fix.Consumer.Grpc;
using Fix.Protocol;
using Grpc.Core;

namespace Fix.Consumer;

/// <summary>
/// gRPC-side fan-out. <see cref="MarketEventsService"/> registers a per-call channel here, and
/// the publisher writes incoming <see cref="FixEvent"/>s into all matching channels. Subscribers
/// drain those channels onto their server-streaming responses.
/// </summary>
public sealed class GrpcEventPublisher : IEventPublisher
{
    private readonly ConcurrentDictionary<string, ChannelWriter<FixEvent>> _writers = new();

    public void Register(string subscriberId, Channel<FixEvent> channel) =>
        _writers[subscriberId] = channel.Writer;

    public void Unregister(string subscriberId)
    {
        if (_writers.TryRemove(subscriberId, out var w)) w.TryComplete();
    }

    public Task PublishAsync(string subscriberId, FixEvent evt, CancellationToken ct)
    {
        if (_writers.TryGetValue(subscriberId, out var w))
            w.TryWrite(evt);
        return Task.CompletedTask;
    }

    public Task CompleteAsync(string subscriberId, CancellationToken ct)
    {
        if (_writers.TryRemove(subscriberId, out var w)) w.TryComplete();
        return Task.CompletedTask;
    }

    /// <summary>Maps a <see cref="FixEvent"/> envelope to the proto-generated DTO.</summary>
    public static FixEventDto ToDto(FixEvent evt)
    {
        var dto = new FixEventDto
        {
            Kind = evt.Kind.ToString(),
            Symbol = evt.Symbol ?? string.Empty,
            SenderCompId = evt.SenderCompId ?? string.Empty,
            TargetCompId = evt.TargetCompId ?? string.Empty,
            MsgSeqNum = evt.MsgSeqNum ?? 0L,
            SendingTimeTicks = evt.SendingTimeTicks ?? 0L,
            Source = evt.Source ?? string.Empty,
        };
        if (evt.Payload is MarketDataPayload md)
        {
            dto.Price = md.Price;
            dto.Quantity = md.Quantity;
            dto.EntryType = md.EntryType ?? string.Empty;
        }
        return dto;
    }
}

/// <summary>
/// Server-streaming gRPC service. Each call registers a per-call channel with
/// <see cref="GrpcEventPublisher"/> and forwards events to the client until cancellation.
/// </summary>
/// <remarks>
/// This service deliberately does not start a <see cref="SubscriptionPump"/> on its own —
/// the SignalR hub remains the single subscription entry point that decides which transport
/// to use. The gRPC service is purely an alternative outbound channel for events that the
/// hub has already subscribed to. Callers therefore need to (1) open a SignalR connection
/// and call <c>Subscribe</c>, and (2) open a gRPC stream with the same <c>subscriberId</c>
/// (the SignalR connection id) to receive events over gRPC. Bridging both into a single
/// API is left as a future extension.
/// </remarks>
public sealed class MarketEventsService : MarketEvents.MarketEventsBase
{
    private readonly GrpcEventPublisher _publisher;

    public MarketEventsService(GrpcEventPublisher publisher) => _publisher = publisher;

    public override async Task Stream(StreamRequest request, IServerStreamWriter<FixEventDto> responseStream, ServerCallContext context)
    {
        var ch = Channel.CreateBounded<FixEvent>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
        _publisher.Register(request.SubscriberId, ch);
        try
        {
            await foreach (var evt in ch.Reader.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
            {
                await responseStream.WriteAsync(GrpcEventPublisher.ToDto(evt), context.CancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            _publisher.Unregister(request.SubscriberId);
        }
    }
}
