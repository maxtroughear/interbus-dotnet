using InterBus.Dispatching;

namespace InterBus.MessagePumps;

internal enum PumpStage { Acquisition, Decode, Dispatch, Settlement, Cleanup, Scope, ProbeDeadline, Completion }
internal enum PumpOutcome { Success, Empty, Failure, Rejected, Canceled, Indeterminate }
internal enum PumpCancellation { None, Shutdown, AcquisitionDeadline }

internal sealed record PumpObservation(Guid OperationId, Guid? DeliveryId, long Generation, bool Probe,
    PumpStage Stage, PumpOutcome Outcome, DateTimeOffset CompletedAt, Exception? Error = null,
    DispatchResult? Dispatch = null, PumpCancellation Cancellation = PumpCancellation.None,
    bool? Recoverable = null, TimeSpan? RetryDelay = null);
