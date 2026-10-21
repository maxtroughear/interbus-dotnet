using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace InterBus.AmazonSqs.Tests;

public partial class SettlementOwnershipRaceTests
{
    private sealed class ControlledSqs(string operation) : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public TaskCompletionSource OperationEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource AllowOperation { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RenewalEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FailRenewal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RenewalExited { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Deletes, ImmediateReleases;
        public override async Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            if (operation == "dead-letter-send")
                await BlockAsync(cancellationToken);
            return new();
        }

        public override async Task<DeleteMessageResponse> DeleteMessageAsync(DeleteMessageRequest request, CancellationToken cancellationToken = default)
        {
            Deletes++;
            if (operation != "dead-letter-send")
                await BlockAsync(cancellationToken);
            return new();
        }

        private async Task BlockAsync(CancellationToken token)
        {
            OperationEntered.TrySetResult();
            await AllowOperation.Task.WaitAsync(token);
        }

        public override async Task<ChangeMessageVisibilityResponse> ChangeMessageVisibilityAsync(ChangeMessageVisibilityRequest request, CancellationToken cancellationToken = default)
        {
            if (request.VisibilityTimeout == 0)
                ImmediateReleases++;
            RenewalEntered.TrySetResult();
            try
            {
                await FailRenewal.Task.WaitAsync(cancellationToken);
                throw new ReceiptHandleIsInvalidException("lost ownership");
            }
            finally
            {
                RenewalExited.TrySetResult();
            }
        }
    }
}
