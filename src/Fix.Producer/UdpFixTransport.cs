using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Fix.Protocol;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Fix.Producer;

/// <summary>
/// UDP transport for the producer. Clients subscribe by sending a FIX Logon datagram with a
/// Symbol (tag 55). The transport maintains a list of (symbol → endpoints) and forwards each
/// generated tick to every endpoint subscribed to that symbol.
/// </summary>
public sealed class UdpFixTransport : BackgroundService, IFixTransport
{
    private readonly IMarketDataSource _source;
    private readonly ProducerOptions _options;
    private readonly ILogger<UdpFixTransport> _logger;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<IPEndPoint, byte>> _subscribers = new();

    public UdpFixTransport(IMarketDataSource source, IOptions<ProducerOptions> options, ILogger<UdpFixTransport> logger)
    {
        _source = source;
        _options = options.Value;
        _logger = logger;
    }

    public string Name => "UDP";

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        _options.Udp.Enabled ? RunAsync(stoppingToken) : Task.CompletedTask;

    public async Task RunAsync(CancellationToken ct)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.Bind(new IPEndPoint(IPAddress.Any, _options.Udp.Port));
        _logger.LogInformation("UDP FIX transport listening on port {Port}", _options.Udp.Port);

        // Spin a fan-out task per symbol; the receive loop manages subscriptions.
        var senderTasks = new List<Task>(_source.Symbols.Count);
        foreach (var symbol in _source.Symbols)
        {
            senderTasks.Add(Task.Run(() => SendLoopAsync(socket, symbol, ct), ct));
        }

        try
        {
            await ReceiveLoopAsync(socket, ct).ConfigureAwait(false);
        }
        finally
        {
            socket.Dispose();
            try { await Task.WhenAll(senderTasks).ConfigureAwait(false); } catch { /* shutdown */ }
        }
    }

    private async Task ReceiveLoopAsync(Socket socket, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(2048);
        var symBuffer = ArrayPool<byte>.Shared.Rent(MarketTick.MaxSymbolLength);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                SocketReceiveFromResult result;
                try
                {
                    result = await socket.ReceiveFromAsync(buffer, SocketFlags.None, new IPEndPoint(IPAddress.Any, 0), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { break; }
                catch (SocketException) { continue; }

                var data = buffer.AsSpan(0, result.ReceivedBytes);
                if (!FixInterpreter.TryGetMsgType(data, out var msgType)) continue;
                if (msgType != FixConstants.MsgTypeLogon) continue;

                var sym = symBuffer.AsSpan(0, MarketTick.MaxSymbolLength);
                if (!FixInterpreter.TryGetSymbol(data, sym, out var symLen)) continue;

                var symbol = System.Text.Encoding.ASCII.GetString(sym[..symLen]);
                if (_source.GetReader(symbol) is null)
                {
                    _logger.LogWarning("UDP {Remote}: unknown symbol {Symbol}", result.RemoteEndPoint, symbol);
                    continue;
                }

                var set = _subscribers.GetOrAdd(symbol, _ => new ConcurrentDictionary<IPEndPoint, byte>());
                set[(IPEndPoint)result.RemoteEndPoint] = 0;
                _logger.LogInformation("UDP {Remote}: subscribed to {Symbol}", result.RemoteEndPoint, symbol);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            ArrayPool<byte>.Shared.Return(symBuffer);
        }
    }

    private async Task SendLoopAsync(Socket socket, string symbol, CancellationToken ct)
    {
        var reader = _source.GetReader(symbol)!;
        var buffer = ArrayPool<byte>.Shared.Rent(256);
        try
        {
            await foreach (var tick in reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                if (!_subscribers.TryGetValue(symbol, out var set) || set.IsEmpty) continue;
                if (!FixInterpreter.TryWriteIncrementalRefresh(in tick, buffer, out var written)) continue;
                var payload = buffer.AsMemory(0, written);
                foreach (var ep in set.Keys)
                {
                    try
                    {
                        await socket.SendToAsync(payload, SocketFlags.None, ep, ct).ConfigureAwait(false);
                    }
                    catch (SocketException)
                    {
                        set.TryRemove(ep, out _);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
