using System.Collections.Concurrent;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace InterBus.AmazonSqs.Tests;

public sealed partial class MessagePumpAdapterTests
{
    private sealed class FakeSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public ConcurrentQueue<Message> Messages { get; } = new();

        public bool FailDelete, FailRenewal, FailSend;
        public SendMessageRequest? LastSend;
        public int Deletes, Renewals, ImmediateReleases;
        public CancellationTokenSource? CancelOnReceive;
        public string? BlockDeleteReceipt;
        public TaskCompletionSource DeleteBlocked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource AllowDelete { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FourRenewed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly ConcurrentDictionary<string, bool> _renewedReceipts = new();
        public TaskCompletionSource Deleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Renewed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            var batch = new List<Message>();
            while (batch.Count < request.MaxNumberOfMessages && Messages.TryDequeue(out var message))
                batch.Add(message);
            if (batch.Count > 0)
            {
                CancelOnReceive?.Cancel();
                return new ReceiveMessageResponse
                {
                    Messages = batch
                };
            }

            await Task.Delay(Timeout.Infinite, cancellationToken);
            return new ReceiveMessageResponse();
        }

        public override async Task<DeleteMessageResponse> DeleteMessageAsync(DeleteMessageRequest request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Deletes);
            if (FailDelete)
                throw new IOException("delete failed");
            if (request.ReceiptHandle == BlockDeleteReceipt)
            {
                DeleteBlocked.TrySetResult();
                await AllowDelete.Task.WaitAsync(cancellationToken);
            }

            Deleted.TrySetResult();
            return new DeleteMessageResponse();
        }

        public override Task<ChangeMessageVisibilityResponse> ChangeMessageVisibilityAsync(ChangeMessageVisibilityRequest request, CancellationToken cancellationToken = default)
        {
            if (request.VisibilityTimeout == 0)
                Interlocked.Increment(ref ImmediateReleases);
            Interlocked.Increment(ref Renewals);
            _renewedReceipts.TryAdd(request.ReceiptHandle, true);
            if (_renewedReceipts.Count == 4)
                FourRenewed.TrySetResult();
            Renewed.TrySetResult();
            if (FailRenewal)
                throw new ReceiptHandleIsInvalidException("lost");
            return Task.FromResult(new ChangeMessageVisibilityResponse());
        }

        public override Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            LastSend = request;
            if (FailSend)
                throw new IOException("send failed");
            return Task.FromResult(new SendMessageResponse());
        }
    }
}
