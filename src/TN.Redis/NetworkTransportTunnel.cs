using System.Net;
using System.Net.Sockets;
using System.Net.Transport;
using RESPite.Transports;
using StackExchange.Redis;
using StackExchange.Redis.Configuration;

namespace TN.Redis;

/// <summary>
/// The SE.Redis <see cref="Tunnel"/> over Dima's proposed client surface
/// (<see cref="NetworkTransportFactory"/>), in either of the two shapes SE.Redis offers:
///   * <see cref="TunnelMode.Transport"/> — <see cref="Tunnel.ConnectTransportAsync"/> yields a
///     <see cref="NetworkDuplexTransport"/>; no socket, no Stream, the engine owns the IO end-to-end.
///   * <see cref="TunnelMode.Stream"/> — the SHIPPED seam: <see cref="Tunnel.GetSocketConnectEndpointAsync"/>
///     returns null (no socket) and <see cref="Tunnel.BeforeAuthenticateAsync"/> returns
///     <see cref="NetworkConnection.AsStream"/>; SE.Redis then runs its ordinary pipe loops over that
///     Stream. Works with the API exactly as proposed, at the cost of the Stream adapter in the path.
///
/// Like SocketSetTunnel, this is the ANCHOR: one engine (one set of worker threads) for every dial this
/// tunnel makes, regardless of endpoints/multiplexers.
/// </summary>
public sealed class NetworkTransportTunnel : Tunnel, IAsyncDisposable
{
    private readonly NetworkTransportFactory _factory;
    private readonly TunnelMode _mode;
    private readonly TransportSendMode _sendMode;
    private readonly bool _completeReceivesInline;

    private readonly long _maxQueuedSendBytes;

    public NetworkTransportTunnel(NetworkEngineOptions options, TunnelMode mode = TunnelMode.Transport,
        TransportSendMode sendMode = TransportSendMode.EnqueueSend, bool completeReceivesInline = true,
        long maxQueuedSendBytes = 0)
    {
        _factory = new NetworkTransportFactory(options);
        _mode = mode;
        _sendMode = sendMode;
        _completeReceivesInline = completeReceivesInline;
        _maxQueuedSendBytes = maxQueuedSendBytes;
    }

    // There is no public CanConnect on the factory (Bedrock gap #3), so the tunnel re-implements the
    // engine's constraints to fall back cleanly: IPv4 (or DNS, which the factory resolves to IPv4).
    private static bool CanConnect(EndPoint endpoint) => endpoint switch
    {
        IPEndPoint ip => ip.AddressFamily == AddressFamily.InterNetwork,
        DnsEndPoint => true,
        _ => false,
    };

    public override async ValueTask<DuplexTransport?> ConnectTransportAsync(
        EndPoint endpoint, ConnectionType connectionType, TlsOptions tls, CancellationToken cancellationToken)
    {
        // The epoll client is plaintext-only; returning null falls back to the ordinary socket path,
        // where SE.Redis applies its own SslStream.
        if (_mode != TunnelMode.Transport || tls.IsEnabled || !CanConnect(endpoint))
        {
            return null;
        }
        var context = await _factory.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        return new NetworkDuplexTransport(context, _sendMode, _completeReceivesInline, _maxQueuedSendBytes);
    }

    public override ValueTask<EndPoint?> GetSocketConnectEndpointAsync(EndPoint endpoint, CancellationToken cancellationToken)
        => _mode == TunnelMode.Stream && CanConnect(endpoint)
            ? new((EndPoint?)null) // no library socket; BeforeAuthenticateAsync supplies the Stream
            : new(endpoint);

    public override async ValueTask<Stream?> BeforeAuthenticateAsync(
        EndPoint endpoint, ConnectionType connectionType, Socket? socket, CancellationToken cancellationToken)
    {
        if (_mode != TunnelMode.Stream || socket is not null || !CanConnect(endpoint))
        {
            return null;
        }
        var context = await _factory.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        var connection = context.Features.Get<INetworkConnectionFeature>()!.Connection;
        connection.CompleteReceivesInline = _completeReceivesInline;
        return new OwningStream(connection.AsStream(), context);
    }

    public ValueTask DisposeAsync() => _factory.DisposeAsync();

    /// <summary>AsStream is non-owning by design; SE.Redis disposes the Stream it was handed, so the
    /// connection's lifetime has to ride along.</summary>
    private sealed class OwningStream(Stream inner, IAsyncDisposable owner) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanWrite => inner.CanWrite;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => inner.Read(buffer);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => inner.ReadAsync(buffer, offset, count, cancellationToken);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => inner.WriteAsync(buffer, cancellationToken);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => inner.WriteAsync(buffer, offset, count, cancellationToken);
        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
                _ = owner.DisposeAsync().AsTask();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync().ConfigureAwait(false);
            await owner.DisposeAsync().ConfigureAwait(false);
            GC.SuppressFinalize(this);
        }
    }
}

public enum TunnelMode
{
    /// <summary>ConnectTransportAsync → DuplexTransport (experimental SER009 seam; no socket, no Stream).</summary>
    Transport = 0,

    /// <summary>BeforeAuthenticateAsync → NetworkConnection.AsStream() (shipped seam; pipe loops over a Stream).</summary>
    Stream = 1,
}
