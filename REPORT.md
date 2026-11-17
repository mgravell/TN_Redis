# Client-side evaluation: `System.Net.Transport` proposal × StackExchange.Redis

Feedback for the Bedrock / `Transport.Networking` prototype
([PUBLIC-API.md](https://github.com/DeagleGross/aspnetcore/blob/dmkorolev/project-bedrock/src/Servers/Kestrel/Transport.Networking/PUBLIC-API.md),
[Bedrock-API.md](https://github.com/DeagleGross/aspnetcore/blob/dmkorolev/project-bedrock/src/Servers/Kestrel/Transport.Networking/Bedrock-API.md)),
from wiring its **client** surface into StackExchange.Redis as a real transport and running it
against a real redis-server. Everything below was observed by running code in this repo, not by
reading the docs.

## What was built

The whole `Transport.Networking` tree was yanked from
`DeagleGross/aspnetcore @ dmkorolev/project-bedrock` (pristine copy in `upstream/`) and built
standalone outside arcade (`src/NetworkTransport.Primitives`, `src/NetworkTransport`; epoll shim
compiled from `native-nossl.c` — the TLS exports stubbed since the client path is plaintext and this
box has no OpenSSL dev headers). On top of that, `src/TN.Redis` plugs the proposal into SE.Redis
through **both** of SE.Redis's transport seams, using **only the proposal's public client API**
(`NetworkTransportFactory.ConnectAsync` → `ConnectionContext` → `INetworkConnectionFeature` →
`NetworkConnection`):

| Seam | SE.Redis API | Mapping |
| --- | --- | --- |
| Stream (shipped) | `Tunnel.GetSocketConnectEndpointAsync → null` + `Tunnel.BeforeAuthenticateAsync → Stream` | `NetworkConnection.AsStream()` |
| Transport (experimental, SER009) | `Tunnel.ConnectTransportAsync → DuplexTransport` | pull→push pump over `ReceiveAsync`/`AdvanceReceiveTo`; sends via `AllocateBuffer`+`EnqueueSend` **or** the `Output` PipeWriter (both exercised) |

The harness (`src/TN.Redis/Program.cs`) runs, per scenario: 64 KiB and 4 MiB SET/GET round-trips,
pub/sub through the second (subscription) connection, server-initiated close via `CLIENT KILL`
followed by reconnect-through-the-tunnel, then sequential awaited PINGs, pipelined INCR batches
(1000 × 55), and 32 parallel callers. Local redis-server, loopback, 4 engine workers, Release,
.NET 11. Numbers are single-machine loopback indications, not benchmarks.

## Results

| scenario | ping µs | pipelined kop/s | parallel kop/s |
| --- | ---: | ---: | ---: |
| sockets (SE.Redis baseline) | 20.7 | 538 | 501 |
| stream tunnel (`AsStream`) | 35.0 | 249 | 357 |
| transport, `EnqueueSend`, 1 MiB budget | **FAILED** (by design: admission) | | |
| transport, `EnqueueSend`, inline receives | 27.5 | 179 | 337 |
| transport, `EnqueueSend`, pool receives | 26.9 | 177 | 388 |
| transport, `Output` pipe, inline receives | 22.5 | **604** | **539** |

Correctness: every passing scenario also passed large-value round-trips, pub/sub, and
kill/reconnect. The API is **client-complete for Redis**: connect, duplex IO, server-initiated
close detection, and re-dial all work through the public surface with no changes to the prototype.

## 1. Things that don't work

### 1.1 `NetworkEngine.DisposeAsync` deadlocks forever on an abandoned connection

**Observed:** after a stream-mode run, `NetworkTransportFactory.DisposeAsync` hung indefinitely.
Dump autopsy: `EpollClient.StopAsync` → `Task.WhenAll(worker.Stopped)` with the two workers that
carried connections never completing. Both `TcpConnection`s were fully closed and drained
(`Closed=1, Leased=0, fd=-1` — `StopConnections` had closed them natively), but the worker loop
exits only when `_connections` is empty, and the **only** removal is the `Forget` command, which is
sent solely by the consumer's `NetworkConnection.DisposeAsync` (`_disposingStarted` was 0: nobody
had disposed them).

The trigger was actually a bug on *our* side — SE.Redis's `PhysicalConnection.Shutdown()` disposed
the socket and the SER009 transport but never `_ioStream`, which is harmless when the stream wraps
a socket but leaks a socketless tunnel stream; fixed in our checkout. But the engine behavior is
the finding: **consumers with teardown bugs exist, and one leaked connection turns engine disposal
into an infinite hang with no diagnostic.** Suggestions, in increasing order of opinion:

* `DisposeAsync` on the engine should forcibly reclaim connections that are already natively closed
  but never consumer-disposed (abort + self-forget), or fail/log after a deadline — anything but a
  silent forever-wait;
* consider making engine dispose the owner of last resort generally (Kestrel's socket transport
  survives abandoned `ConnectionContext`s);
* see also 2.4 (`AsStream` ownership), which is how this class of leak happens in practice.

### 1.2 The direct-send admission budget converts big payloads into connection corruption

`EnqueueSend`/`SendAsync` count bytes against `MaxQueuedSendBytes` (default 1 MiB) and **throw**
`InvalidOperationException` on overflow ("an admission limit, not a wait" — PROTOTYPE.md). For a
stage-then-flush client (SE.Redis, and anything with a fire-and-forget write model) this has two
problems, both observed:

* A single 4 MiB `SET` deterministically kills the connection ("transport enqueue 1 MiB budget" row
  above). There is no way to await admission; the only options are to size the budget for the
  largest conceivable flush (we had to pass 16 MiB) or to fail.
* Worse: the throw can land **mid-batch**, after part of a framed command was already queued. The
  connection is still "healthy" at the transport level, but the protocol stream is torn — without
  consumer heroics (our adapter calls `Abort` on overflow) you get a silent desync and a timeout
  rather than a crisp failure.

Suggestions: an awaitable admission path (`ValueTask EnqueueSendAsync(...)` or a
`WaitToSendAsync()` gate) so flow control is expressible; a dedicated exception type; and the
budget on the options/factory rather than only as a post-connect property race. At minimum,
document the partial-frame hazard: "a send that would exceed it throws" undersells what that does
to a framed protocol.

### 1.3 No client TLS

`NetworkNativeTlsOptions` is listener-only, and the epoll client workers are constructed
`tls: false`. For Redis (and most client scenarios) TLS is table stakes; today the tunnel must
return `null` and fall back to sockets+`SslStream` whenever `Ssl` is configured. Even just the
Bedrock answer — "wrap `AsStream()` in `SslStream` yourself" — deserves a stated position, because
it reintroduces the Stream adapter cost that the transport seam exists to avoid (see stream-tunnel
row: +70% ping latency, roughly half the throughput).

### 1.4 IPv4 TCP only, and no way to ask

`EpollClient.ConnectAsync` throws for anything but IPv4 `IPEndPoint`s; no IPv6, no Unix domain
sockets (very common for local Redis; SocketSet dials UDS including abstract-namespace). Combined
with Bedrock's own gap #3 (no `CanConnect` counterpart to `CanBind`), a consumer cannot even ask —
our tunnel re-implements the engine's constraints by hand to decide when to fall back. A
`CanConnect(EndPoint)` plus UDS support would cover the real client matrix.

## 2. Things that could be improved

### 2.1 The direct send queue needs coalescing — it currently loses to the Output pipe by 3×

This is the most interesting performance result. Identical workload, syscall counts via
`strace -c`:

| | `EnqueueSend` mode | `Output` pipe mode |
| --- | ---: | ---: |
| `sendto` calls | **86,036** | **9,587** |
| pipelined throughput | 179 kop/s | 604 kop/s |

`PumpOutputCore` in direct mode submits strictly one request at a time (pin → `SubmitSendCore` →
wait completion → next) and never merges adjacent queued requests, so every flushed slice is its
own wire write. The pipe path wins because the `Pipe` itself merges flushes into large segments —
and additionally only the pipe path can ever set `final: true`, which is what arms `TCP_CORK` in
`TcpConnection.Send`; direct sends never cork. Net effect: the API's "designed for this"
fire-and-forget path is 9× the syscalls and ~3× slower than the adapter it was meant to beat,
on exactly the pipelined small-write workload a Redis client produces.

Suggestions: gather-submit the queued requests (`writev`/`sendmsg` across request boundaries), or
copy-merge small adjacent slices on the worker, and/or expose the cork/`final` hint on
`EnqueueSend` (SE.Redis's `Flush()` is precisely that boundary; the `DuplexTransport` contract
even hands it to us — we just have nowhere to put it).

Meanwhile the pipe-output row is genuinely strong: it **beat SE.Redis's tuned socket baseline on
both throughput tests** (+12% pipelined, +8% parallel) at ~2 µs ping cost. The client story does
not need new machinery to be competitive — it needs the direct path to catch up with the pipe path,
or the guidance to just say "clients: use `Output`".

### 2.2 Pull-based receive forces a re-inversion for push consumers

SE.Redis's transport contract (like SocketSet's) is push: the transport delivers spans on its
schedule, with an explicit batch-end callback for flush coalescing. `ReceiveAsync` is pull, so the
adapter runs a pump loop re-inverting control. It works, and two design choices deserve explicit
credit:

* `ReceiveAsync` returning *everything unconsumed* as one multi-segment snapshot gives a natural
  batch boundary — we map snapshot-end to `OnBatchEnd` and it behaves;
* `CompleteReceivesInline` makes the pump's resumption run on the epoll worker, so delivery is
  effectively push-on-worker — the SocketSet shape — and it is a per-connection property, which is
  exactly the right granularity.

Still, each batch pays an async state-machine cycle plus `AdvanceReceiveTo` bookkeeping. A
first-class push option (`connection.StartReceiving(IReceiveHandler)` with a loop-drain/batch-end
notification, mirroring the worker's own event batches) would make the inline mode cheaper and the
"batch" boundary real rather than approximated. On loopback the measured residual is ~2–7 µs per
RTT vs sockets; fine, but it is pure adapter tax.

### 2.3 One copy is structural for pull-parsing clients

A Redis client parses out of a contiguous rolling buffer, so transport-owned pages get copied once
into the parser's buffer (same as every SE.Redis transport today — not a regression). The zero-copy
story (`Retain`/`NetworkPayload`) only pays off for consumers that can parse segmented sequences in
place and hold leases. Worth stating in the docs which consumers the retained-lease path is for;
for SE.Redis the win was never the copy, it is scheduling (2.2) and syscalls (2.1).

### 2.4 `AsStream()` ownership

`AsStream()` is deliberately non-owning, but a Stream is frequently the *only* handle a consumer
layer gets (SE.Redis tunnels, `SslStream`, `ConnectCallback` in Bedrock itself — that is why
`ConnectionContextStream` exists). Every such consumer must hand-write an owning wrapper or leak
(see 1.1 — we hit exactly this). An `AsStream(ownsConnection: true)` overload is cheap and removes
the sharpest edge of the shipped-seam path.

### 2.5 Smaller client-surface nits

* **Blocking `connect()` on a pool thread** (`EpollClient`): acknowledged as unmeasured-path
  pragmatism; fine for a prototype, but a real client API needs non-blocking connect on the worker
  plus a connect-timeout option (today cancellation is the only control, and SE.Redis passes
  `CancellationToken.None` to tunnels — the hang in 1.1 was undiagnosable partly because *nothing*
  in the dial path times out).
* **Client receive window / page sizes** are env vars (`NETWORKPROTO_CLIENT_LEASE_BYTES`,
  `NETWORKPROTO_CLIENT_PAGE_KB`); the Bedrock doc already says the first few should become options
  — agreed, they are the client analogue of `WindowsEngineOptions` pool knobs.
* **Teardown-race ergonomics**: during shutdown, `EnqueueSend` throws `IOException` and the
  `Output` pipe writer completes; a consumer-side `bool TryEnqueueSend(...)` (or documented
  closed-state behavior) would avoid try/catch as control flow in every adapter's flush path.
* **`CompleteReceivesInline` showed no measurable latency win here** (27.5 µs inline vs 26.9 µs
  pool on loopback ping) — unsurprising for a single-connection RTT test on loopback; the server
  numbers in Bedrock.md tell the real story. Mentioned only so nobody reads this evaluation as
  contradicting those.

## 3. What worked well (worth keeping exactly as-is)

* **The Bedrock client surface is sufficient.** `NetworkTransportFactory` (`IConnectionFactory`) +
  `INetworkConnectionFeature` was enough to build a complete SE.Redis transport — both seams, both
  send modes — with zero modifications to the prototype. DNS endpoints resolve; `ConnectionClosed`
  and the close paths behave; pub/sub's second connection and `CLIENT KILL` reconnects all flow
  through the tunnel correctly.
* **`AllocateBuffer` + `NetworkBufferSlice` + `EnqueueSend` is the right *shape*** for a
  stage-then-flush writer: refcounted pages, fire-and-forget with failure-by-closure, order
  preserved, caller releases immediately. SocketSet converged on the same shape independently
  (`transport IS the IBufferWriter` + explicit `Flush`), which is good evidence the shape is right
  — it is only the submission coalescing behind it that lags (2.1).
* **`CompleteReceivesInline` as a per-connection property** composes perfectly with a multiplexing
  client that wants parse-on-worker for data connections but default scheduling elsewhere.
* **Engine-per-factory anchoring** maps cleanly onto the "one engine, N loop threads total,
  regardless of topology" model that SocketSet's SE.Redis tunnel established; thread count stays a
  configuration constant across any number of endpoints/multiplexers.

## Repro

```
cd src/TN.Redis && dotnet run -c Release            # full matrix (spawns redis-server on :6399)
dotnet run -c Release -- 6399 enqueue               # scenario filter (substring)
TN_VERBOSE=1 ...                                    # SE.Redis connect log to stdout
```

Needs: linux, `cc`, net11 SDK, sibling `StackExchange.Redis` checkout (SER009 + the
`Shutdown()`/`_ioStream` fix described in 1.1, currently an uncommitted change in that checkout),
and `/home/marc/code/redis/src/redis-server` or `REDIS_SERVER` env var.
