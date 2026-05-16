using Fix.Protocol;

namespace Fix.Consumer;

/// <summary>
/// Outbound fan-out abstraction. Decouples the connection-side <see cref="IMarketConnection"/>
/// pump from the wire format that delivers events to a particular client. Concrete
/// implementations target SignalR, gRPC, Redis pub/sub, or anything else.
/// </summary>
/// <remarks>
/// The same <c>subscriberId</c> identifies one logical subscriber across the lifetime of a
/// subscription. For SignalR it is the SignalR connection id; for gRPC it is the per-call
/// streaming id; for Redis it is a free-form correlation id chosen by the caller.
/// </remarks>
public interface IEventPublisher
{
    /// <summary>Push a single event to the subscriber.</summary>
    Task PublishAsync(string subscriberId, FixEvent evt, CancellationToken ct);

    /// <summary>Signal that no more events will be published for this subscriber.</summary>
    Task CompleteAsync(string subscriberId, CancellationToken ct);
}
