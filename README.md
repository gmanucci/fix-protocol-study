# fix-protocol-study

A small end-to-end FIX-protocol study built on .NET 10 + Angular 20. It demonstrates a
zero-allocation hot path for market-data fan-out from a producer (TCP & UDP) to multiple
consumers, which then re-publish to a browser through SignalR.

## Layout

```
src/
  Fix.Protocol/        # shared FIX 4.4 subset encoder/decoder, MarketTick struct
  Fix.Producer/        # worker host: singleton market generator + TCP & UDP transports
  Fix.Consumer/        # ASP.NET Core host: TCP/UDP client connections + SignalR hub
  Fix.Consumer.Web/    # Angular 20 SPA that subscribes via SignalR
tests/
  Fix.Protocol.Tests/
  Fix.Producer.Tests/
  Fix.Consumer.Tests/
Fix.sln
```

## Requirements

- .NET 10 SDK (`dotnet --version` → 10.x)
- Node.js 20+ and npm 10+

## Build & test

```bash
dotnet build
dotnet test            # 12 tests across the three test projects
cd src/Fix.Consumer.Web && npm install && npm run build
```

## Run the full stack

In three terminals:

```bash
# 1) Producer (TCP :5010, UDP :5011)
dotnet run --project src/Fix.Producer

# 2) Consumer (SignalR hub at /hub/market on :5000)
ASPNETCORE_URLS=http://localhost:5000 dotnet run --project src/Fix.Consumer

# 3) Angular dev server (proxies /hub to the consumer)
cd src/Fix.Consumer.Web && npm start
# open http://localhost:4200
```

## Configuration

`src/Fix.Producer/appsettings.json`:

```json
"Producer": {
  "Markets": ["EURUSD", "GBPUSD", "USDJPY", "AAPL", "MSFT"],
  "TickIntervalMicroseconds": 10000,
  "ChannelCapacity": 1024,
  "Tcp": { "Enabled": true, "Port": 5010 },
  "Udp": { "Enabled": true, "Port": 5011 }
}
```

`src/Fix.Consumer/appsettings.json`:

```json
"Consumer": {
  "ProducerHost": "127.0.0.1",
  "ProducerTcpPort": 5010,
  "ProducerUdpPort": 5011,
  "ChannelCapacity": 1024,
  "CorsOrigins": ["http://localhost:4200"]
}
```

## Design notes

### Struct vs record for `MarketTick`

The hot path moves a `MarketTick` value through a bounded `Channel<T>` and through
encoder/decoder routines that work on `Span<byte>`. We compared the three obvious choices:

| Choice          | Allocation per tick | Boxing risk in `Channel<T>` | Memory layout control |
|-----------------|--------------------|-----------------------------|-----------------------|
| `class record`  | Heap-allocated     | None (T is `record`)        | None                  |
| `record struct` | None (stack/inline)| None                        | Limited (compiler-gen)|
| `readonly struct` (chosen) | None    | None                        | Full (`StructLayout`) |

We picked **`struct`** (a fixed-layout value type with a `fixed byte[8]` symbol buffer):
no GC pressure on the hot path, blittable, no managed references, and zero compiler-synthesised
overhead. `record` is reserved for cold-path DTOs (configuration, the SignalR `TickDto`).

### GC discipline

- `System.Threading.Channels.Channel<T>` of value types — no boxing, no per-message allocation.
- `ArrayPool<byte>.Shared` for FIX encode/decode buffers in transports and connections.
- `System.IO.Pipelines` for the TCP read path on the consumer side.
- `Utf8Parser` / `Utf8Formatter` instead of `string.Format` / `int.Parse`.
- `BoundedChannelFullMode.DropOldest` — keep latency low instead of stalling on backpressure.

To squeeze more out of the runtime, configure ServerGC for both producer and consumer:

```xml
<PropertyGroup>
  <ServerGarbageCollection>true</ServerGarbageCollection>
  <ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
  <TieredPGO>true</TieredPGO>
</PropertyGroup>
```

### FIX dialect

A study-grade subset of FIX 4.4 is implemented:

- `8=FIX.4.4` BeginString, `9=` BodyLength, `10=` CheckSum (mod-256).
- `35=A` Logon (used by UDP clients to subscribe to a symbol).
- `35=X` MarketDataIncrementalRefresh with tags 52, 55, 269, 270, 271.

Session-layer compliance (sequence numbers, gap fill, resend) is intentionally out of scope.

### Architecture

```
+-----------------+            FIX over TCP             +-----------------+    SignalR JSON     +---------+
| Fix.Producer    |  ---> 5010 (Stream)  -------------> | Fix.Consumer    |  ---> /hub/market   | Angular |
|                 |                                     | Tcp/UdpMarket-  |                    |  app    |
| MarketDataSource|  ---> 5011 (Dgram, Logon-subscribe) | Connection      |                    +---------+
| (singleton,     |  ---> ChannelReader<MarketTick>     | + MarketHub     |
|  one channel    |                                     | (singleton fac- |
|  per symbol)    |                                     |  tory of scoped |
+-----------------+                                     |  connections)   |
                                                        +-----------------+
```
