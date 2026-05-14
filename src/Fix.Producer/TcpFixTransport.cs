using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Fix.Protocol;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Fix.Producer;

/// <summary>
/// TCP transport for the producer. Each accepted connection sends a one-line ASCII subscription
/// request "SUB &lt;SYMBOL&gt;\n", then receives a stream of FIX MarketDataIncrementalRefresh
/// messages for that symbol until the connection closes.
/// </summary>
public sealed class TcpFixTransport : BackgroundService, IFixTransport
{
    private readonly IMarketDataSource _source;
    private readonly ProducerOptions _options;
    private readonly ILogger<TcpFixTransport> _logger;

    public TcpFixTransport(IMarketDataSource source, IOptions<ProducerOptions> options, ILogger<TcpFixTransport> logger)
    {
        _source = source;
        _options = options.Value;
        _logger = logger;
    }

    public string Name => "TCP";

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        _options.Tcp.Enabled ? RunAsync(stoppingToken) : Task.CompletedTask;

    public async Task RunAsync(CancellationToken ct)
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true,
        };
        listener.Bind(new IPEndPoint(IPAddress.Any, _options.Tcp.Port));
        listener.Listen(128);
        _logger.LogInformation("TCP FIX transport listening on port {Port}", _options.Tcp.Port);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await listener.AcceptAsync(ct).ConfigureAwait(false);
                _ = Task.Run(() => HandleClientAsync(client, ct), ct);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            listener.Dispose();
        }
    }

    private async Task HandleClientAsync(Socket client, CancellationToken ct)
    {
        client.NoDelay = true;
        var remote = client.RemoteEndPoint;
        try
        {
            // Read subscription line: "SUB <SYMBOL>\n"
            var symbol = await ReadSubscriptionAsync(client, ct).ConfigureAwait(false);
            if (symbol is null)
            {
                _logger.LogWarning("TCP {Remote}: bad subscription, closing", remote);
                return;
            }

            var reader = _source.GetReader(symbol);
            if (reader is null)
            {
                _logger.LogWarning("TCP {Remote}: unknown symbol {Symbol}", remote, symbol);
                return;
            }

            _logger.LogInformation("TCP {Remote}: subscribed to {Symbol}", remote, symbol);

            var buffer = ArrayPool<byte>.Shared.Rent(256);
            try
            {
                await foreach (var tick in reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    if (!FixInterpreter.TryWriteIncrementalRefresh(in tick, buffer, out var written))
                        continue;
                    await client.SendAsync(buffer.AsMemory(0, written), SocketFlags.None, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) when (ex is SocketException or IOException or ObjectDisposedException)
        {
            _logger.LogInformation("TCP {Remote}: disconnected ({Reason})", remote, ex.GetType().Name);
        }
        finally
        {
            try { client.Shutdown(SocketShutdown.Both); } catch { /* best effort */ }
            client.Dispose();
        }
    }

    private static async Task<string?> ReadSubscriptionAsync(Socket client, CancellationToken ct)
    {
        var buf = ArrayPool<byte>.Shared.Rent(64);
        try
        {
            int total = 0;
            while (total < buf.Length)
            {
                var read = await client.ReceiveAsync(buf.AsMemory(total), SocketFlags.None, ct).ConfigureAwait(false);
                if (read <= 0) return null;
                total += read;
                var nl = Array.IndexOf(buf, (byte)'\n', 0, total);
                if (nl < 0) continue;
                var line = Encoding.ASCII.GetString(buf, 0, nl).Trim();
                if (!line.StartsWith("SUB ", StringComparison.Ordinal)) return null;
                return line[4..].Trim();
            }
            return null;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }
}
