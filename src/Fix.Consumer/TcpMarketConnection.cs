using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Fix.Protocol;
using Microsoft.Extensions.Logging;

namespace Fix.Consumer;

/// <summary>
/// TCP-backed market connection. Connects to the producer, sends a "SUB &lt;SYMBOL&gt;\n" line,
/// and frames incoming FIX messages off a <see cref="PipeReader"/> into the local
/// <see cref="Channel{T}"/> of <see cref="MarketTick"/>.
/// </summary>
public sealed class TcpMarketConnection : IMarketConnection
{
    private readonly ConsumerOptions _options;
    private readonly ILogger<TcpMarketConnection> _logger;
    private readonly Channel<FixEvent> _channel;
    private IReadOnlySet<FixEventKind> _kinds = new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh };
    private Socket? _socket;
    private CancellationTokenSource? _cts;
    private Task? _runner;

    public TcpMarketConnection(string symbol, ConsumerOptions options, ILogger<TcpMarketConnection> logger)
    {
        Symbol = symbol;
        _options = options;
        _logger = logger;
        _channel = Channel.CreateBounded<FixEvent>(new BoundedChannelOptions(_options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
    }

    public string Symbol { get; }
    public string Transport => "TCP";
    public ChannelReader<FixEvent> Events => _channel.Reader;

    public async Task StartAsync(IReadOnlySet<FixEventKind> subscribedKinds, CancellationToken ct)
    {
        if (subscribedKinds.Count > 0)
            _kinds = subscribedKinds;
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await _socket.ConnectAsync(_options.ProducerHost, _options.ProducerTcpPort, ct).ConfigureAwait(false);
        await _socket.SendAsync(Encoding.ASCII.GetBytes($"SUB {Symbol}\n"), SocketFlags.None, ct).ConfigureAwait(false);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _runner = Task.Run(() => RunAsync(_cts.Token), _cts.Token);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        var pipe = new Pipe();
        var fill = FillPipeAsync(_socket!, pipe.Writer, ct);
        var read = ReadPipeAsync(pipe.Reader, ct);
        try
        {
            await Task.WhenAll(fill, read).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "TCP connection for {Symbol} terminated", Symbol);
        }
        finally
        {
            _channel.Writer.TryComplete();
        }
    }

    private static async Task FillPipeAsync(Socket socket, PipeWriter writer, CancellationToken ct)
    {
        const int minBuf = 512;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var memory = writer.GetMemory(minBuf);
                int read;
                try { read = await socket.ReceiveAsync(memory, SocketFlags.None, ct).ConfigureAwait(false); }
                catch (SocketException) { break; }
                if (read == 0) break;
                writer.Advance(read);
                var flush = await writer.FlushAsync(ct).ConfigureAwait(false);
                if (flush.IsCompleted) break;
            }
        }
        finally
        {
            await writer.CompleteAsync().ConfigureAwait(false);
        }
    }

    private async Task ReadPipeAsync(PipeReader reader, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct).ConfigureAwait(false);
                var buffer = result.Buffer;

                while (TryReadOne(ref buffer, out var tick, out var parsed))
                {
                    if (parsed && _kinds.Contains(FixEventKind.MarketDataIncrementalRefresh))
                        await _channel.Writer.WriteAsync(MarketTickEvents.ToIncrementalRefresh(tick, Transport), ct).ConfigureAwait(false);
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tries to extract one complete FIX message from <paramref name="buffer"/>. Returns
    /// <c>true</c> when a complete message boundary was found and consumed (the buffer is
    /// sliced past it). The <paramref name="parsed"/> flag indicates whether the message
    /// successfully decoded into a <see cref="MarketTick"/>; malformed messages are skipped
    /// rather than re-tried so the framer makes forward progress.
    /// </summary>
    private static bool TryReadOne(ref ReadOnlySequence<byte> buffer, out MarketTick tick, out bool parsed)
    {
        tick = default;
        parsed = false;
        if (buffer.IsEmpty) return false;

        if (buffer.IsSingleSegment)
        {
            var span = buffer.First.Span;
            var len = FixInterpreter.TryFindMessageBoundary(span);
            if (len <= 0) return false;
            parsed = FixInterpreter.TryParseIncrementalRefresh(span[..len], out tick);
            buffer = buffer.Slice(len);
            return true;
        }
        else
        {
            var max = checked((int)Math.Min(buffer.Length, 4096));
            byte[] rented = ArrayPool<byte>.Shared.Rent(max);
            try
            {
                buffer.Slice(0, max).CopyTo(rented);
                var len = FixInterpreter.TryFindMessageBoundary(rented.AsSpan(0, max));
                if (len <= 0) return false;
                parsed = FixInterpreter.TryParseIncrementalRefresh(rented.AsSpan(0, len), out tick);
                buffer = buffer.Slice(len);
                return true;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try { _cts?.Cancel(); } catch { }
        try { _socket?.Shutdown(SocketShutdown.Both); } catch { }
        _socket?.Dispose();
        if (_runner is not null)
        {
            try { await _runner.ConfigureAwait(false); } catch { }
        }
        _cts?.Dispose();
    }
}
