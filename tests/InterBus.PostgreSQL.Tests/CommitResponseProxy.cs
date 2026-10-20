using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace InterBus.PostgreSQL.Tests;

internal sealed class CommitResponseProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentBag<Task> _connections = [];
    private readonly ConcurrentDictionary<TcpClient, byte> _clients = new();
    private readonly string _host;
    private readonly int _port;
    private readonly Task _accept;
    private int _armed;
    public TaskCompletionSource DroppedCommit { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    public ConcurrentQueue<Exception> Errors { get; } = new();

    public CommitResponseProxy(string host, int port)
    {
        _host = host;
        _port = port;
        _listener.Start();
        _accept = AcceptAsync();
    }

    public void DropNextCommitResponse() => Interlocked.Exchange(ref _armed, 1);

    private async Task AcceptAsync()
    {
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                _clients.TryAdd(client, 0);
                _connections.Add(ForwardAsync(client));
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (SocketException) when (_stop.IsCancellationRequested) { }
    }

    private async Task ForwardAsync(TcpClient client)
    {
        using (client)
        using (var backend = new TcpClient())
        using (var connection = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token))
        {
            try
            {
                await backend.ConnectAsync(_host, _port, connection.Token);
                var upstream = client.GetStream().CopyToAsync(backend.GetStream(), connection.Token);
                var downstream = CopyResponsesAsync(backend.GetStream(), client.GetStream(), connection.Token);
                await Task.WhenAny(upstream, downstream);
                await connection.CancelAsync();
                client.Close();
                backend.Close();
                await ObserveAsync(upstream);
                await ObserveAsync(downstream);
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
            catch (Exception error) { Errors.Enqueue(error); }
            finally { _clients.TryRemove(client, out _); }
        }
    }

    private async Task ObserveAsync(Task task)
    {
        try { await task; }
        catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
        catch (Exception error) { Errors.Enqueue(error); }
    }

    private async Task CopyResponsesAsync(Stream backend, Stream client, CancellationToken token)
    {
        var header = new byte[5];
        while (await backend.ReadAsync(header.AsMemory(0, 1), token) != 0)
        {
            await backend.ReadExactlyAsync(header.AsMemory(1, 4), token);
            var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1)) - 4;
            if (length is < 0 or > 64 * 1024 * 1024) throw new InvalidDataException("Invalid PostgreSQL response frame");
            var body = new byte[length];
            await backend.ReadExactlyAsync(body, token);
            // CommandComplete is emitted only after the server has committed; dropping it makes the client outcome ambiguous
            if (header[0] == (byte)'C' && body.AsSpan().SequenceEqual("COMMIT\0"u8) &&
                Interlocked.CompareExchange(ref _armed, 0, 1) == 1)
            {
                DroppedCommit.TrySetResult();
                return;
            }
            await client.WriteAsync(header, token);
            await client.WriteAsync(body, token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _accept;
        foreach (var client in _clients.Keys) client.Dispose();
        await Task.WhenAll(_connections);
        _stop.Dispose();
    }
}
