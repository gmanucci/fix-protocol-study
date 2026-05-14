namespace Fix.Producer;

/// <summary>
/// Common interface for the producer's network transports. Each implementation is registered as
/// an <see cref="Microsoft.Extensions.Hosting.IHostedService"/> and runs its own accept/serve loop.
/// </summary>
public interface IFixTransport
{
    string Name { get; }
    Task RunAsync(CancellationToken ct);
}
