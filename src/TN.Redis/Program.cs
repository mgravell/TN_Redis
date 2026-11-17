using System.Diagnostics;
using System.Net.Transport;
using StackExchange.Redis;
using StackExchange.Redis.Configuration;
using TN.Redis;

// Evaluation harness: SE.Redis against a local redis-server through Dima's proposed client surface,
// in every seam/mode combination, with the stock socket path as the baseline.
//
// usage: TN.Redis [port]   (default 6399; starts its own redis-server unless one is already there)

var port = args.Length > 0 && int.TryParse(args[0], out var p) ? p : 6399;
var tlsPort = port + 1;
var filter = args.Length > 1 ? args[1] : null; // substring match on scenario name
var verbose = Environment.GetEnvironmentVariable("TN_VERBOSE") == "1";
var workers = Math.Min(4, Environment.ProcessorCount);

await using var server = await LocalRedis.EnsureAsync(port, tlsPort);
Console.WriteLine($"redis: {server.Describe}, engine workers: {workers}");
Console.WriteLine();

var results = new List<(string Name, string Status, double PingUs, double KOpsBatch, double KOpsParallel)>();

foreach (var (name, tunnel, tls) in Scenarios())
{
    if (filter is not null && !name.Contains(filter, StringComparison.OrdinalIgnoreCase))
    {
        continue;
    }
    try
    {
        var config = new ConfigurationOptions
        {
            EndPoints = { { "127.0.0.1", tls ? tlsPort : port } },
            Tunnel = tunnel,
            AbortOnConnectFail = true,
            ConnectTimeout = 5000,
            Ssl = tls,
        };
        if (tls)
        {
            config.CertificateValidation += (_, _, _, _) => true; // self-signed test cert
        }
        Console.WriteLine($"[{name}] connecting...");
        await using var muxer = verbose
            ? await ConnectionMultiplexer.ConnectAsync(config, Console.Out).WaitAsync(TimeSpan.FromSeconds(30))
            : await ConnectionMultiplexer.ConnectAsync(config).WaitAsync(TimeSpan.FromSeconds(30));
        var result = await ExerciseAsync(muxer, name);
        results.Add(result);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[{name}] FAILED: {ex.Message}");
        results.Add((name, $"FAILED: {ex.GetType().Name}", 0, 0, 0));
    }
    finally
    {
        if (tunnel is IAsyncDisposable disposable)
        {
            await disposable.DisposeAsync();
        }
    }
}

Console.WriteLine();
Console.WriteLine($"{"scenario",-34} {"status",-10} {"ping µs",10} {"batch kop/s",12} {"parallel kop/s",15}");
foreach (var (name, status, ping, batch, parallel) in results)
{
    Console.WriteLine($"{name,-34} {status,-10} {ping,10:F1} {batch,12:F0} {parallel,15:F0}");
}

IEnumerable<(string Name, Tunnel? Tunnel, bool Tls)> Scenarios()
{
    const long RaisedBudget = 16 * 1024 * 1024;
    yield return ("sockets (baseline)", null, false);
    yield return ("stream tunnel", new NetworkTransportTunnel(Options(), TunnelMode.Stream), false);
    yield return ("transport enqueue 1MiB budget", new NetworkTransportTunnel(Options(), TunnelMode.Transport, TransportSendMode.EnqueueSend, completeReceivesInline: true), false); // expected: 4MiB smoke test trips the admission budget
    yield return ("transport enqueue inline", new NetworkTransportTunnel(Options(), TunnelMode.Transport, TransportSendMode.EnqueueSend, completeReceivesInline: true, RaisedBudget), false);
    yield return ("transport enqueue pool", new NetworkTransportTunnel(Options(), TunnelMode.Transport, TransportSendMode.EnqueueSend, completeReceivesInline: false, RaisedBudget), false);
    yield return ("transport pipe-output inline", new NetworkTransportTunnel(Options(), TunnelMode.Transport, TransportSendMode.PipeOutput, completeReceivesInline: true), false);
    // TLS: the engine has no client TLS, so the transport seam cannot participate (the tunnel
    // falls back to sockets when TLS is demanded). What CAN be measured: SE.Redis's own SslStream
    // over the proposal's AsStream() — the stream seam — against SslStream-over-socket.
    yield return ("sockets + TLS (baseline)", null, true);
    yield return ("stream tunnel + TLS", new NetworkTransportTunnel(Options(), TunnelMode.Stream), true);

    NetworkEngineOptions Options() => new EpollEngineOptions { WorkerCount = workers };
}

async Task<(string, string, double, double, double)> ExerciseAsync(ConnectionMultiplexer muxer, string name)
{
    var db = muxer.GetDatabase();

    // --- correctness smoke tests ---
    if (!await db.PingAsync() .ContinueWith(t => t.IsCompletedSuccessfully)) throw new InvalidOperationException("ping failed");
    var key = (RedisKey)$"tn:{name}:{Guid.NewGuid():N}";
    foreach (var size in new[] { 64 * 1024, 4 * 1024 * 1024 }) // 4 MiB deliberately exceeds the 1 MiB direct-send admission budget
    {
        var value = new byte[size];
        Random.Shared.NextBytes(value);
        await db.StringSetAsync(key, value);
        var fetched = (byte[]?)await db.StringGetAsync(key);
        if (fetched is null || !fetched.AsSpan().SequenceEqual(value)) throw new InvalidOperationException($"{size / 1024}KiB SET/GET round-trip mismatch");
        await db.KeyDeleteAsync(key);
    }

    // pub/sub exercises the second (subscription) connection through the same tunnel
    var channel = RedisChannel.Literal($"tn:{name}");
    var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    var sub = muxer.GetSubscriber();
    await sub.SubscribeAsync(channel, (_, message) => received.TrySetResult(message!));
    await sub.PublishAsync(channel, "hello");
    var echoed = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
    if (echoed != "hello") throw new InvalidOperationException("pub/sub mismatch");
    await sub.UnsubscribeAsync(channel);

    // server-initiated close: have redis kill this muxer's connections, then verify the bridge
    // observes the close (pump -> OnClosed) and SE.Redis reconnects through the tunnel
    await using (var admin = await ConnectionMultiplexer.ConnectAsync(new ConfigurationOptions
    {
        EndPoints = { { "127.0.0.1", port } },
        AllowAdmin = true,
    }))
    {
        var adminServer = admin.GetServer("127.0.0.1", port);
        await adminServer.ExecuteAsync("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes");
        await adminServer.ExecuteAsync("CLIENT", "KILL", "TYPE", "pubsub", "SKIPME", "yes");
    }
    var recovered = false;
    for (var i = 0; i < 100 && !recovered; i++)
    {
        try
        {
            await db.PingAsync();
            recovered = true;
        }
        catch (RedisException)
        {
            await Task.Delay(100);
        }
    }
    if (!recovered) throw new InvalidOperationException("no reconnect after CLIENT KILL");

    // --- sequential latency: awaited PINGs ---
    const int PingCount = 2_000;
    for (var i = 0; i < 200; i++) { await db.PingAsync(); } // warmup
    var watch = Stopwatch.StartNew();
    for (var i = 0; i < PingCount; i++) { await db.PingAsync(); }
    watch.Stop();
    var pingMicros = watch.Elapsed.TotalMicroseconds / PingCount;

    // --- pipelined throughput: batches of concurrent INCRs on one logical caller ---
    const int BatchSize = 1_000, Batches = 50;
    var counter = (RedisKey)$"tn:{name}:ctr";
    var tasks = new Task[BatchSize];
    for (var b = 0; b < 5; b++) { await RunBatchAsync(); } // warmup
    watch.Restart();
    for (var b = 0; b < Batches; b++) { await RunBatchAsync(); }
    watch.Stop();
    var batchKops = BatchSize * Batches / watch.Elapsed.TotalSeconds / 1_000;

    // --- parallel callers: 32 tasks each running awaited GET/SET pairs ---
    const int Callers = 32, OpsPerCaller = 2_000;
    watch.Restart();
    await Task.WhenAll(Enumerable.Range(0, Callers).Select(async caller =>
    {
        var k = (RedisKey)$"tn:{name}:p{caller}";
        for (var i = 0; i < OpsPerCaller; i++)
        {
            await db.StringSetAsync(k, i, flags: CommandFlags.FireAndForget);
            await db.StringGetAsync(k);
        }
    }));
    watch.Stop();
    var parallelKops = Callers * OpsPerCaller * 2 / watch.Elapsed.TotalSeconds / 1_000;

    await db.KeyDeleteAsync(counter);
    Console.WriteLine($"[{name}] ok: ping {pingMicros:F1}µs, batch {batchKops:F0}kop/s, parallel {parallelKops:F0}kop/s");
    return (name, "ok", pingMicros, batchKops, parallelKops);

    async Task RunBatchAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            tasks[i] = db.StringIncrementAsync(counter);
        }
        await Task.WhenAll(tasks);
    }
}

/// <summary>Starts a throwaway redis-server if nothing is listening on the port.</summary>
file sealed class LocalRedis : IAsyncDisposable
{
    private Process? _process;
    public string Describe { get; private init; } = "";

    public static async Task<LocalRedis> EnsureAsync(int port, int tlsPort)
    {
        using (var probe = new System.Net.Sockets.TcpClient())
        {
            try
            {
                await probe.ConnectAsync("127.0.0.1", port);
                return new LocalRedis { Describe = $"existing server on :{port}" };
            }
            catch (System.Net.Sockets.SocketException)
            {
            }
        }
        var binary = Environment.GetEnvironmentVariable("REDIS_SERVER") ?? "/home/marc/code/redis/src/redis-server";
        var certs = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../certs"));
        var tlsArgs = File.Exists(Path.Combine(certs, "cert.pem"))
            ? $" --tls-port {tlsPort} --tls-cert-file {Path.Combine(certs, "cert.pem")} --tls-key-file {Path.Combine(certs, "key.pem")} --tls-auth-clients no"
            : "";
        var process = Process.Start(new ProcessStartInfo(binary,
            $"--port {port}{tlsArgs} --save \"\" --appendonly no --daemonize no --loglevel warning")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        }) ?? throw new InvalidOperationException("failed to start redis-server");
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(50);
            using var probe = new System.Net.Sockets.TcpClient();
            try
            {
                await probe.ConnectAsync("127.0.0.1", port);
                return new LocalRedis { _process = process, Describe = $"spawned {binary} on :{port} (pid {process.Id})" };
            }
            catch (System.Net.Sockets.SocketException)
            {
            }
        }
        try { process.Kill(); } catch { }
        throw new TimeoutException($"redis-server did not come up on :{port}");
    }

    public ValueTask DisposeAsync()
    {
        if (_process is { } process)
        {
            try { process.Kill(); process.WaitForExit(3000); } catch { }
            process.Dispose();
        }
        return default;
    }
}
