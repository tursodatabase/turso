namespace Turso;

public enum TursoAutomaticSyncState
{
    Stopped,
    Waiting,
    Running,
    Retrying,
    Faulted,
}

public sealed record TursoAutomaticSyncStatus(
    TursoAutomaticSyncState State,
    int Attempt,
    DateTimeOffset? LastAttempt,
    DateTimeOffset? LastSuccess,
    bool? LastPullAppliedChanges,
    Exception? LastException,
    DateTimeOffset? NextAttempt)
{
    public static TursoAutomaticSyncStatus Stopped { get; } = new(
        TursoAutomaticSyncState.Stopped,
        Attempt: 0,
        LastAttempt: null,
        LastSuccess: null,
        LastPullAppliedChanges: null,
        LastException: null,
        NextAttempt: null);
}

public sealed class TursoAutomaticSyncStatusChangedEventArgs(TursoAutomaticSyncStatus status) : EventArgs
{
    public TursoAutomaticSyncStatus Status { get; } = status;
}
