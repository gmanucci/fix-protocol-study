using Fix.Protocol;
using Microsoft.AspNetCore.SignalR;

namespace Fix.Consumer;

/// <summary>
/// Default <see cref="IEventPublisher"/>: forwards each <see cref="FixEvent"/> to the SignalR
/// connection whose id equals the supplied <c>subscriberId</c>. Sends two messages:
/// <list type="bullet">
///   <item><c>"event"</c> — the full <see cref="FixEvent"/> as JSON.</item>
///   <item><c>"tick"</c> — a legacy <see cref="TickDto"/> when the event is a market-data refresh,
///         preserved for backwards compatibility with the original Angular client.</item>
/// </list>
/// </summary>
public sealed class SignalREventPublisher : IEventPublisher
{
    private readonly IHubContext<MarketHub> _hub;

    public SignalREventPublisher(IHubContext<MarketHub> hub) => _hub = hub;

    public async Task PublishAsync(string subscriberId, FixEvent evt, CancellationToken ct)
    {
        var client = _hub.Clients.Client(subscriberId);
        await client.SendAsync("event", evt, ct).ConfigureAwait(false);

        if (evt.Kind == FixEventKind.MarketDataIncrementalRefresh && evt.Payload is MarketDataPayload md)
        {
            var legacy = new TickDto(
                Symbol: evt.Symbol ?? string.Empty,
                Price: md.Price,
                Quantity: md.Quantity,
                TimestampTicks: evt.SendingTimeTicks ?? 0L,
                EntryType: md.EntryType,
                Transport: evt.Source);
            await client.SendAsync("tick", legacy, ct).ConfigureAwait(false);
        }
    }

    public Task CompleteAsync(string subscriberId, CancellationToken ct) => Task.CompletedTask;
}
