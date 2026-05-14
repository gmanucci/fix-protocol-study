using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Fix.Protocol;
using Microsoft.Extensions.Logging;

namespace Fix.Consumer;

/// <summary>
/// UDP-backed market connection. Sends a Logon datagram identifying the symbol, then publishes
/// every received MarketDataIncrementalRefresh into the channel.
/// </summary>
public sealed class UdpMarketConnection : IMarketConnection
{
    private readonly ConsumerOptions _options;
    private readonly ILogger<UdpMarketConnection> _logger;
    private readonly Channel<MarketTick> _channel;
    private Socket? _socket;
    private CancellationTokenSource? _cts;
    private Task? _runner;

    public UdpMarketConnection(string symbol, ConsumerOptions options, ILogger<UdpMarketConnection> logger)
    {
        Symbol = symbol;
        _options = options;
        _logger = logger;
        _channel = Channel.CreateBounded<MarketTick>(new BoundedChannelOptions(_options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
    }

    public string Symbol { get; }
    public string Transport => "UDP";
    public ChannelReader<MarketTick> Ticks => _channel.Reader;

    public async Task StartAsync(CancellationToken ct)
    {
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _socket.Bind(new IPEndPoint(IPAddress.Any, 0));
        var producerEp = new IPEndPoint(IPAddress.Parse(_options.ProducerHost), _options.ProducerUdpPort);

        // Send the FIX Logon to subscribe.
        var logon = ArrayPool<byte>.Shared.Rent(128);
        try
        {
            if (!FixInterpreter.TryWriteLogon(System.Text.Encoding.ASCII.GetBytes(Symbol), logon, out var written))
                throw new InvalidOperationException("Failed to write logon");
            await _socket.SendToAsync(logon.AsMemory(0, written), SocketFlags.None, producerEp, ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(logon);
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _runner = Task.Run(() => RunAsync(producerEp, _cts.Token), _cts.Token);
    }

    private async Task RunAsync(IPEndPoint producerEp, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(2048);
        // Re-send Logon periodically to keep the producer subscription alive.
        var logonBuf = ArrayPool<byte>.Shared.Rent(128);
        FixInterpreter.TryWriteLogon(System.Text.Encoding.ASCII.GetBytes(Symbol), logonBuf, out var logonLen);
        var nextRefresh = Environment.TickCount64 + 5_000;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (Environment.TickCount64 >= nextRefresh)
                {
                    try { await _socket!.SendToAsync(logonBuf.AsMemory(0, logonLen), SocketFlags.None, producerEp, ct).ConfigureAwait(false); }
                    catch { /* best effort */ }
                    nextRefresh = Environment.TickCount64 + 5_000;
                }

                SocketReceiveFromResult result;
                try
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    timeoutCts.CancelAfter(2_000);
                    result = await _socket!.ReceiveFromAsync(buffer, SocketFlags.None, new IPEndPoint(IPAddress.Any, 0), timeoutCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested) { continue; }
                catch (OperationCanceledException) { break; }
                catch (SocketException) { continue; }

                if (FixInterpreter.TryParseIncrementalRefresh(buffer.AsSpan(0, result.ReceivedBytes), out var tick))
                {
                    await _channel.Writer.WriteAsync(tick, ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "UDP connection for {Symbol} terminated", Symbol);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            ArrayPool<byte>.Shared.Return(logonBuf);
            _channel.Writer.TryComplete();
        }
    }

    public async ValueTask DisposeAsync()
    {
        try { _cts?.Cancel(); } catch { }
        _socket?.Dispose();
        if (_runner is not null)
        {
            try { await _runner.ConfigureAwait(false); } catch { }
        }
        _cts?.Dispose();
    }
}
