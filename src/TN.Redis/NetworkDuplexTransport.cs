using System.Buffers;
using System.Net.Transport;
using Microsoft.AspNetCore.Connections;
using RESPite.Transports;

namespace TN.Redis;

/// <summary>
/// SE.Redis's <see cref="DuplexTransport"/> implemented over a Bedrock <see cref="NetworkConnection"/>,
/// reached through the PUBLIC client surface only: <c>NetworkTransportFactory.ConnectAsync</c> →
/// <see cref="ConnectionContext"/> → <see cref="INetworkConnectionFeature"/>. This is the fit test for
/// Dima's proposed API on the client side: everything the adapter cannot express cleanly is an API
/// finding, not an implementation choice.
///
/// Outbound: two selectable mappings, because the proposal has two exclusive send modes.
///   * <see cref="TransportSendMode.EnqueueSend"/> — stage into <see cref="NetworkConnection.AllocateBuffer"/>
///     storage; <see cref="Flush"/> queues the staged slices via <see cref="NetworkConnection.EnqueueSend(NetworkBufferSlice)"/>
///     (fire-and-forget, refcounted pages, no await): the natural match for SE.Redis's
///     stage-then-flush writer.
///   * <see cref="TransportSendMode.PipeOutput"/> — stage into <see cref="NetworkConnection.Output"/>
///     (the PipeWriter); <see cref="Flush"/> is an unobserved <c>FlushAsync</c>.
///
/// Inbound: the proposal is PULL (<see cref="NetworkConnection.ReceiveAsync"/>), SE.Redis's transport
/// contract is PUSH, so a pump loop re-inverts control. With
/// <see cref="NetworkConnection.CompleteReceivesInline"/> the pump's continuation runs on the epoll
/// worker that read the bytes, so delivery is effectively push-on-worker (the SocketSet shape); without
/// it every batch costs a thread-pool hop.
/// </summary>
public sealed class NetworkDuplexTransport : DuplexTransport
{
    private readonly ConnectionContext _context;     // owns the connection's lifetime
    private readonly NetworkConnection _connection;
    private readonly TransportSendMode _sendMode;
    private TransportReceiver? _receiver;
    private int _closed; // OnClosed must fire exactly once

    // EnqueueSend staging: filled-but-unflushed segments, oldest first, plus the buffer being filled.
    // DuplexTransport's contract is a single logical writer, so no locks here; _closed is the only
    // cross-thread flag.
    private readonly List<(NetworkBuffer Buffer, int Start, int End)> _staged = new();
    private NetworkBuffer? _current;
    private int _start, _written; // flushed-up-to and staged-up-to within _current

    public NetworkDuplexTransport(ConnectionContext context, TransportSendMode sendMode, bool completeReceivesInline,
        long maxQueuedSendBytes = 0)
    {
        _context = context;
        _connection = context.Features.Get<INetworkConnectionFeature>()?.Connection
            ?? throw new InvalidOperationException("The connection does not expose INetworkConnectionFeature; the pipe adapter would be the only path.");
        _connection.CompleteReceivesInline = completeReceivesInline;
        if (maxQueuedSendBytes > 0)
        {
            // The direct-send admission budget (default 1 MiB) is a THROW, not a wait; a fire-and-forget
            // writer like SE.Redis has nowhere to await, so the budget has to be sized for the largest
            // flush instead. See the report.
            _connection.MaxQueuedSendBytes = maxQueuedSendBytes;
        }
        _sendMode = sendMode;
    }

    public override bool IsEncrypted => _connection.IsTls;

    // ---- outbound ----

    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_sendMode == TransportSendMode.PipeOutput)
        {
            return _connection.Output.GetMemory(sizeHint);
        }
        var needed = Math.Max(sizeHint, 1);
        if (_current is { } current && current.Memory.Length - _written >= needed)
        {
            return current.Memory.Slice(_written);
        }
        // Retire the partially-filled buffer (its staged tail flushes later) and start a fresh one.
        if (_current is not null && _written > _start)
        {
            _staged.Add((_current, _start, _written));
        }
        else
        {
            _current?.Dispose();
        }
        _current = _connection.AllocateBuffer(needed);
        _start = _written = 0;
        return _current.Memory;
    }

    public override void Advance(int count)
    {
        if (_sendMode == TransportSendMode.PipeOutput)
        {
            _connection.Output.Advance(count);
        }
        else
        {
            _written += count;
        }
    }

    public override bool Flush()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            return false;
        }
        try
        {
            if (_sendMode == TransportSendMode.PipeOutput)
            {
                // DuplexTransport.Flush is synchronous and non-blocking; the pipe's backpressure
                // surface (FlushResult) has nowhere to land. See the report: this mapping discards it.
                _ = _connection.Output.FlushAsync();
                return true;
            }
            foreach (var (buffer, start, end) in _staged)
            {
                _connection.EnqueueSend(buffer.Slice.Slice(start, end - start));
                buffer.Dispose(); // EnqueueSend retained its own reference
            }
            _staged.Clear();
            if (_current is { } current && _written > _start)
            {
                _connection.EnqueueSend(current.Slice.Slice(_start, _written - _start));
                _start = _written; // keep filling the same page; flushed bytes are never rewritten
            }
            return true;
        }
        catch (Exception ex) // IOException once closing, InvalidOperationException over budget
        {
            // An admission failure can strike mid-batch, after part of a framed command was already
            // queued: the connection is alive but the protocol stream is now torn, so the only safe
            // reaction is to kill the connection (which also routes the failure to OnClosed).
            _connection.Abort(ex);
            return false;
        }
    }

    // ---- inbound ----

    public override void Start(TransportReceiver receiver)
    {
        if (Interlocked.CompareExchange(ref _receiver, receiver, null) is not null)
        {
            throw new InvalidOperationException("receiver already started");
        }
        _ = Task.Run(PumpAsync);
    }

    private async Task PumpAsync()
    {
        var connection = _connection;
        var receiver = _receiver!;
        try
        {
            while (true)
            {
                // One "batch" = one snapshot of everything unconsumed. With CompleteReceivesInline the
                // await resumes on the worker that received the bytes.
                var result = await connection.ReceiveAsync().ConfigureAwait(false);
                var buffer = result.Buffer;
                var keep = true;
                if (!buffer.IsEmpty)
                {
                    foreach (var segment in buffer)
                    {
                        if (!receiver.OnReceived(segment.Span))
                        {
                            keep = false;
                            break;
                        }
                    }
                }
                connection.AdvanceReceiveTo(buffer.End);
                receiver.OnBatchEnd();
                if (!keep)
                {
                    connection.Abort();
                    return; // Abort surfaces through a final canceled/faulted read → OnClosed below
                }
                if (result.IsCompleted || result.IsCanceled)
                {
                    SignalClosed(null);
                    return;
                }
            }
        }
        catch (Exception fault)
        {
            SignalClosed(fault);
        }
    }

    private void SignalClosed(Exception? fault)
    {
        if (Interlocked.Exchange(ref _closed, 1) == 0)
        {
            _receiver?.OnClosed(fault);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        Volatile.Write(ref _closed, 1);
        foreach (var (buffer, _, _) in _staged)
        {
            buffer.Dispose();
        }
        _staged.Clear();
        _current?.Dispose();
        _current = null;
        // Abort rather than graceful-close: a pending ReceiveAsync parks the pump, and the graceful
        // DisposeAsync path waits for it; Abort releases it first. (See report: teardown ordering.)
        _connection.Abort();
        await _context.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>Which of the proposal's two (mutually exclusive) send modes the adapter drives.</summary>
public enum TransportSendMode
{
    /// <summary>Stage in <c>AllocateBuffer</c> storage, flush via <c>EnqueueSend</c> (direct queued sends).</summary>
    EnqueueSend = 0,

    /// <summary>Stage in the <c>Output</c> PipeWriter, flush via unobserved <c>FlushAsync</c>.</summary>
    PipeOutput = 1,
}
