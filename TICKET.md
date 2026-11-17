# Client-side evaluation: the `System.Net.Transport` surface as a StackExchange.Redis transport

We wired the prototype's **client** surface into StackExchange.Redis as a real transport and ran it
against a real redis-server, to see how the proposed API holds up for client-side networking (per
[PUBLIC-API.md](https://github.com/DeagleGross/aspnetcore/blob/dmkorolev/project-bedrock/src/Servers/Kestrel/Transport.Networking/PUBLIC-API.md)
and
[Bedrock-API.md](https://github.com/DeagleGross/aspnetcore/blob/dmkorolev/project-bedrock/src/Servers/Kestrel/Transport.Networking/Bedrock-API.md)).
Everything below was observed by running code against this branch, not by reading the docs. Offered
as input ahead of taking the design further — the headline is genuinely positive, and the sharp
edges listed are all fixable at the API level.

## TL;DR

* **The Bedrock client surface is sufficient**: `NetworkTransportFactory.ConnectAsync` →
  `ConnectionContext` → `INetworkConnectionFeature` → `NetworkConnection` was enough to build a
  complete SE.Redis transport — two different integration seams, both send modes — with **zero
  changes to the prototype**. Connect, duplex IO, server-initiated close detection, pub/sub's
  second connection and automatic reconnect all work through the public API.
* **Best configuration beats SE.Redis's tuned socket baseline** on this workload: pipelined
  throughput ~1.0M op/s vs ~575k (median of 3 runs), parallel ~625k vs ~595k, at ~1–2 µs added
  ping latency.
* Four hard problems for client use: engine disposal can deadlock forever on an abandoned
  connection; the direct-send admission budget breaks framed protocols; no client TLS; IPv4 TCP
  only with no `CanConnect`.
* One big measured surprise: the direct send queue (`EnqueueSend`) is **3–5× slower** than the
  `Output` pipe on pipelined load, because it never coalesces — 86k `sendto` syscalls where the
  pipe path does 9.6k for the same workload.

## What was built

StackExchange.Redis has two transport seams, and we exercised the prototype through both:

| Seam | SE.Redis API | Mapping onto the proposal |
| --- | --- | --- |
| Stream (shipped) | `Tunnel.GetSocketConnectEndpointAsync → null` + `Tunnel.BeforeAuthenticateAsync → Stream` | `NetworkConnection.AsStream()` |
| Transport (experimental) | `Tunnel.ConnectTransportAsync → DuplexTransport` (a push receiver + `IBufferWriter` staging with an explicit `Flush()`) | pull→push pump over `ReceiveAsync`/`AdvanceReceiveTo`; sends via `AllocateBuffer`+`EnqueueSend` **or** the `Output` PipeWriter (both measured) |

Per scenario the harness runs: 64 KiB and 4 MiB SET/GET round-trips, pub/sub on the subscription
connection, `CLIENT KILL` → reconnect-through-the-tunnel, then sequential awaited PINGs (latency),
pipelined INCR batches (1000 × 55, single logical caller) and 32 parallel callers doing awaited
GET/SET pairs. Epoll provider, 4 workers; local redis-server over loopback; Release; .NET 11;
Ryzen 9 7900X (24 threads), Linux 7.2.6. Loopback single-machine numbers — indications, not
benchmarks.

## Results (median of 3 runs, min–max in brackets)

| scenario | ping µs | pipelined kop/s | parallel kop/s |
| --- | --- | --- | --- |
| sockets (SE.Redis baseline) | 19.6 [19.3–20.6] | 573 [571–582] | 596 [578–597] |
| stream seam (`AsStream`) | 27.2 [21.8–32.4] | 344 [309–357] | 376 [323–458] |
| transport seam, `EnqueueSend`, default 1 MiB budget | **connection failed by design — see issue 2** | | |
| transport seam, `EnqueueSend`, inline receives | 25.3 [20.2–25.6] | 189 [185–202] | 363 [341–486] |
| transport seam, `EnqueueSend`, pool receives | 23.7 [20.0–24.9] | 198 [195–226] | 581 [381–658] |
| transport seam, `Output` pipe, inline receives | 21.4 [19.9–22.9] | **1011 [639–1061]** | **624 [566–685]** |
| sockets + TLS (`SslStream` over socket) | 24.8 [24.8–26.8] | 764 [634–1063] | 536 [517–545] |
| stream seam + TLS (`SslStream` over `AsStream()`) | 24.8 [24.8–25.0] | 429 [427–438] | 230 [225–258] |

TLS notes: the engine has no client TLS (issue 3), so the transport seam cannot participate at all
— the only measurable TLS shape is SE.Redis's own `SslStream` over `AsStream()`, i.e. the Stream
adapter path, which gives up roughly half the throughput against `SslStream`-over-socket. Also
worth noticing: sockets+TLS *beats plaintext sockets* on the pipelined column — `SslStream`'s
write buffering coalesces small writes into larger TCP segments. Coalescing dominates this
workload, which is exactly the mechanism behind issue 5.

Every passing scenario also passed the large-value, pub/sub and kill/reconnect checks.

## Hard problems

### 1. `NetworkEngine.DisposeAsync` deadlocks forever on an abandoned connection

A consumer teardown bug on our side (since fixed —
[seredis/StackExchange.Redis#3361](https://github.com/seredis/StackExchange.Redis/pull/3361):
SE.Redis didn't dispose a socketless tunnel stream on shutdown) left two `NetworkConnection`s
undisposed. Engine disposal then hung **forever**: dump autopsy showed `EpollClient.StopAsync` →
`Task.WhenAll(worker.Stopped)` never completing, with both `TcpConnection`s already fully closed
and drained at the native level (`Closed=1, Leased=0, fd=-1` — `StopConnections` had closed them)
but still present in the worker's table, because the only removal is the `Forget` command, sent
solely by the consumer's `NetworkConnection.DisposeAsync` (`_disposingStarted` was 0).

Ill-behaved consumers exist, and a Stream-only consumer is especially easy to get wrong (see
issue 7). Suggestions, in increasing order of opinion: on engine dispose, forcibly reclaim
connections that are natively closed but never consumer-disposed (abort + self-forget); or fail/log
after a deadline — anything but a silent infinite wait; more broadly, consider making the engine
the owner of last resort, the way the stock socket transport survives abandoned
`ConnectionContext`s.

### 2. The direct-send admission budget breaks framed protocols

`EnqueueSend`/`SendAsync` count bytes against `MaxQueuedSendBytes` (default 1 MiB) and **throw**
on overflow — "an admission limit, not a wait" (PROTOTYPE.md). For a stage-then-flush client with
a fire-and-forget write model, both consequences were observed:

* a single 4 MiB `SET` deterministically kills the connection (the FAILED row above); there is no
  way to *await* admission, so the only options are sizing the budget for the largest conceivable
  flush (we pass 16 MiB) or failing;
* worse, the throw can land **mid-batch**, after part of a framed command is already queued. The
  connection is still "healthy" at the transport level while the protocol stream is torn — unless
  the consumer aborts the connection on overflow (our adapter does), the result is a silent
  desync and a timeout rather than a crisp failure.

Suggestions: an awaitable admission path (`EnqueueSendAsync`, or a `WaitToSendAsync()` gate);
a dedicated exception type; the budget on options/factory rather than only a settable
post-connect property; and documenting the partial-frame hazard — "a send that would exceed it
throws" undersells what that does to a framed protocol.

### 3. TLS is server-only

The transport-owned TLS work is extensive — native TLS on every provider, three Linux variants,
Schannel with handshake offload on Windows — but it is all **listener-side**. On the client side,
every layer says no:

* `NetworkNativeTlsOptions` is plumbed only through `ListenAsync` ("bound listeners only");
* all three client hosts hardcode it off: `EpollClient` builds workers `tls: false`, `RingClient`
  builds engines `tls: false`, `WindowsClient` constructs its host with `nativeTls: null`;
* the managed helper is one-sided too: `NetworkTlsExtensions` has `AuthenticateAsServerAsync` but
  no `AuthenticateAsClientAsync`;
* Bedrock's own client path punts by design — `UseConnectionFactory` is documented "HTTP framing,
  pooling and TLS stay in the handler", i.e. `SocketsHttpHandler` runs its own `SslStream` over
  the pipe→Stream adapter.

So a TLS client today means `SslStream` over `AsStream()` — which works (the "stream seam + TLS"
row above), but reintroduces exactly the Stream-adapter cost the transport seam exists to avoid:
measured at roughly half the throughput of `SslStream`-over-socket for this workload. For Redis —
and most client scenarios — TLS is table stakes, so the client story needs either engine TLS on
outbound connections (the server machinery looks reusable) or, at minimum, an
`AuthenticateAsClientAsync` counterpart plus a stated position on the cost.

### 4. IPv4 TCP only, and no way to ask

`EpollClient.ConnectAsync` throws for anything but IPv4 `IPEndPoint`s — no IPv6, no Unix domain
sockets (very common for local Redis). Combined with Bedrock-API.md's own gap #3 (no `CanConnect`
counterpart to `CanBind`), a consumer cannot even ask: our tunnel re-implements the engine's
constraints by hand to decide when to fall back to sockets. `CanConnect(EndPoint)` plus UDS
support would cover the real client matrix.

## Measured improvement opportunities

### 5. The direct send queue never coalesces — it loses to the `Output` pipe by 3–5×

The most interesting performance result. Identical workload, syscall counts via `strace -c`:

| | `EnqueueSend` mode | `Output` pipe mode |
| --- | --- | --- |
| `sendto` calls | **86,036** | **9,587** |
| pipelined throughput | ~190 kop/s | ~1,000 kop/s |

`PumpOutputCore` in direct mode submits strictly one request at a time (pin → `SubmitSendCore` →
wait for completion → next) and never merges adjacent queued requests, so every flushed slice is
its own wire write. The pipe path wins because the `Pipe` merges flushes into large segments — and
additionally only the pipe path ever passes `final: true`, which is what arms `TCP_CORK` in
`TcpConnection.Send`; direct sends never cork. Net effect: the API's designed-for-this
fire-and-forget path does 9× the syscalls and runs 3–5× slower than the pipe adapter it was meant
to beat, on exactly the pipelined small-write workload a Redis client produces.

Suggestions: gather-submit queued requests (`writev`/`sendmsg` across request boundaries), or
copy-merge small adjacent slices on the worker, and/or expose the cork/`final` hint on
`EnqueueSend` — a client's `Flush()` is precisely that boundary, and SE.Redis's transport contract
hands it to the adapter; there is currently nowhere to put it.

To be clear about the flip side: the `Output`-pipe client configuration is genuinely strong — it
beat the tuned socket baseline on both throughput columns. The client story doesn't need new
machinery to be competitive; it needs the direct path to catch up with the pipe path, or guidance
that simply says "clients: use `Output`".

### 6. Pull-based receive forces a re-inversion for push consumers

SE.Redis's transport contract (like other loop-thread clients) is push: spans delivered on the
transport's schedule, with a batch-end callback for flush coalescing. `ReceiveAsync` is pull, so
the adapter runs a pump loop. Two design choices deserve explicit credit: the
snapshot-of-everything-unconsumed result gives a natural batch boundary (snapshot-end maps to the
client's batch-end callback and behaves correctly), and `CompleteReceivesInline` makes the pump
resume on the epoll worker — effectively push-on-worker, at exactly the right per-connection
granularity. Still, each batch pays an async state-machine cycle plus cursor bookkeeping; a
first-class push option (`StartReceiving(handler)` with a real loop-drain notification) would make
inline mode cheaper and the batch boundary real rather than approximated. Measured residual on
loopback: ~2–7 µs per RTT over sockets.

(Related observation: a Redis-style client parses out of a contiguous rolling buffer, so one copy
from transport pages into the parser is structural — the retained-lease zero-copy path only pays
off for consumers that can parse segmented sequences in place. Worth a sentence in the docs about
which consumers it targets; for us the win was never the copy, it is scheduling and syscalls.)

### 7. `AsStream()` ownership

`AsStream()` is deliberately non-owning, but a Stream is frequently the *only* handle a consumer
layer gets (tunnel seams, `SslStream`, `SocketsHttpHandler.ConnectCallback` — the reason
`ConnectionContextStream` exists in Bedrock). Every such consumer must hand-write an owning
wrapper or leak — issue 1 is exactly this failure class in the wild. An
`AsStream(ownsConnection: true)` overload is cheap and removes the sharpest edge of the
shipped-seam path.

### 8. Smaller client-surface nits

* **Blocking `connect()` on a pool thread** (`EpollClient`): fine for a prototype, but a real
  client API wants non-blocking connect on the worker plus a connect-timeout option — today
  cancellation is the only control, and tunnel consumers commonly pass `CancellationToken.None`;
  the issue-1 hang was harder to diagnose because nothing in the dial path times out.
* **Client receive window / page sizes** are env vars (`NETWORKPROTO_CLIENT_LEASE_BYTES`,
  `NETWORKPROTO_CLIENT_PAGE_KB`); Bedrock-API.md already says the first few should become options
  — agreed; they are the client analogue of the Windows pool knobs.
* **Teardown-race ergonomics**: during shutdown `EnqueueSend` throws `IOException`; a
  `bool TryEnqueueSend(...)` (or documented closed-state behavior) would avoid try/catch as
  control flow in every adapter's flush path.
* **`CompleteReceivesInline` showed no measurable latency win in this setup** (loopback,
  single-connection RTT) — noted only so this isn't read as contradicting the server numbers in
  Bedrock.md, which measure a different thing.

## What worked well — keep as-is

* `NetworkTransportFactory` + `INetworkConnectionFeature` as *the* client entry point: sufficient,
  discoverable, and the `ConnectionContext` wrapper cost nothing we could measure.
* `AllocateBuffer` + `NetworkBufferSlice` + `EnqueueSend` is the right **shape** for a
  stage-then-flush writer: refcounted pages, fire-and-forget with failure-by-closure, order
  preserved, caller releases immediately. (An independently-developed client transport converged
  on the same shape; only the submission coalescing behind it lags — issue 5.)
* `CompleteReceivesInline` as a per-connection property composes perfectly with a multiplexing
  client that wants parse-on-worker for data connections and default scheduling elsewhere.
* Engine-per-factory anchoring: one engine, N loop threads total, regardless of how many
  endpoints/multiplexers — thread count stays a configuration constant.

## Repro

The evaluation harness is at **[mgravell/TN_Redis](https://github.com/mgravell/TN_Redis)**: a
pristine copy of this branch's `Transport.Networking` tree, a standalone build of it (outside
arcade), and the SE.Redis tunnel/adapter/benchmark. It needs Linux, a net11 SDK, a redis-server
binary, and a sibling StackExchange.Redis checkout at or after
[seredis/StackExchange.Redis#3361](https://github.com/seredis/StackExchange.Redis/pull/3361); the
README has the details.
