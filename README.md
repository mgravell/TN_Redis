# TN_Redis — the `System.Net.Transport` proposal as a StackExchange.Redis client transport

An evaluation harness: the client surface of the Kestrel `Transport.Networking` prototype
([DeagleGross/aspnetcore @ dmkorolev/project-bedrock](https://github.com/DeagleGross/aspnetcore/tree/dmkorolev/project-bedrock/src/Servers/Kestrel/Transport.Networking)),
built standalone and wired into [StackExchange.Redis](https://github.com/seredis/StackExchange.Redis)
as a real transport, run against a real redis-server.

The findings write-up is in [TICKET.md](TICKET.md) (filed upstream as an issue); a longer-form
version with the investigation narrative is in [REPORT.md](REPORT.md).

## Layout

| Path | What |
| --- | --- |
| `upstream/` | Pristine extraction of `src/Servers/Kestrel/Transport.Networking` from the branch above (MIT, .NET Foundation), plus the shared `DuplexPipe.cs` it links. Reference only — nothing here is modified. |
| `src/NetworkTransport.Primitives/`, `src/NetworkTransport/` | Standalone builds of the upstream sources (compiled by link, outside the aspnetcore arcade build). The epoll native shim is built from `native-nossl.c` — the upstream `native.c` with the OpenSSL-dependent TLS exports stubbed, since the client path is plaintext-only and this avoids needing OpenSSL dev headers. |
| `src/TN.Redis/` | The bridge and harness: `NetworkTransportTunnel` (a SE.Redis `Tunnel` over `NetworkTransportFactory`, in both the shipped Stream seam and the experimental SER009 `DuplexTransport` seam), `NetworkDuplexTransport` (the pull→push adapter over `NetworkConnection`, with `EnqueueSend` and `Output`-pipe send modes), and `Program.cs` (the scenario matrix). |

## Running

Requirements:

* Linux, `cc`, .NET 11 SDK;
* a `redis-server` binary (set `REDIS_SERVER`, or edit the default path in `Program.cs`);
* a **sibling checkout** of StackExchange.Redis at `../StackExchange.Redis`, at or after
  [seredis/StackExchange.Redis#3361](https://github.com/seredis/StackExchange.Redis/pull/3361)
  (the socketless-tunnel-stream dispose fix; without it, stream-seam runs deadlock engine
  disposal — that is finding 1 in the ticket);
* for the TLS scenarios, a throwaway self-signed cert:
  `mkdir -p certs && cd certs && openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 30 -subj "/CN=localhost"`
  (without it the harness runs, TLS scenarios just fail to connect; redis must also be built with
  `BUILD_TLS=yes`).

```
cd src/TN.Redis
dotnet run -c Release                 # full matrix; spawns redis-server on :6399 (+:6400 TLS)
dotnet run -c Release -- 6399 enqueue # substring filter on scenario names
TN_VERBOSE=1 ...                      # SE.Redis connect log to stdout
```

Scenario matrix: sockets baseline, Stream seam (`AsStream`), transport seam with `EnqueueSend`
(default and raised admission budget, inline and thread-pool receive completion), transport seam
with the `Output` pipe, plus TLS variants of baseline and Stream seam. Each scenario runs
correctness smoke tests (64 KiB / 4 MiB round-trips, pub/sub, `CLIENT KILL` → reconnect) before
latency and throughput measurements.

## Provenance / license

`upstream/` is MIT-licensed .NET Foundation code, copied verbatim for reference and standalone
compilation. The bridge/harness code in `src/TN.Redis` and the build glue are offered under the
same MIT terms.
