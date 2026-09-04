namespace Turso;

internal sealed class TursoAutomaticSyncCoordinator : IDisposable
{
    private readonly TursoSyncDatabase _database;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource? _cancellation;
    private readonly Task? _task;
    private TursoAutomaticSyncStatus _status = TursoAutomaticSyncStatus.Stopped;
    private readonly System.Collections.Concurrent.ConcurrentQueue<Notification> _notifications = new();
    private int _notificationDrainScheduled;
    private int _disposed;

    public TursoAutomaticSyncCoordinator(
        TursoSyncDatabase database,
        TimeSpan interval,
        TimeProvider timeProvider)
    {
        _database = database;
        _interval = interval;
        _timeProvider = timeProvider;
        if (interval <= TimeSpan.Zero)
            return;

        _cancellation = new CancellationTokenSource();
        Publish(_status with
        {
            State = TursoAutomaticSyncState.Waiting,
            NextAttempt = _timeProvider.GetUtcNow() + interval,
        });
        _task = RunAsync(_cancellation.Token);
    }

    public TursoAutomaticSyncStatus Status => Volatile.Read(ref _status);

    public event EventHandler<TursoAutomaticSyncStatusChangedEventArgs>? StatusChanged;

    public Exception? Stop()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return Status.LastException;
        if (_cancellation is null || _task is null)
            return null;

        try
        {
            _cancellation.Cancel();
            _task.GetAwaiter().GetResult();
            PublishStopped();
            return null;
        }
        catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
        {
            PublishStopped();
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
        finally
        {
            _cancellation.Dispose();
        }
    }

    public void Dispose()
    {
        _ = Stop();
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await Task.Delay(_interval, _timeProvider, cancellationToken).ConfigureAwait(false);
            for (var attempt = 0; ; attempt++)
            {
                var attemptTime = _timeProvider.GetUtcNow();
                Publish(Status with
                {
                    State = attempt == 0
                        ? TursoAutomaticSyncState.Running
                        : TursoAutomaticSyncState.Retrying,
                    Attempt = attempt + 1,
                    LastAttempt = attemptTime,
                    LastException = null,
                    NextAttempt = null,
                });
                try
                {
                    var appliedChanges = await _database.PullAsync(cancellationToken).ConfigureAwait(false);
                    var successTime = _timeProvider.GetUtcNow();
                    Publish(Status with
                    {
                        State = TursoAutomaticSyncState.Waiting,
                        Attempt = 0,
                        LastSuccess = successTime,
                        LastPullAppliedChanges = appliedChanges,
                        LastException = null,
                        NextAttempt = successTime + _interval,
                    });
                    break;
                }
                catch (Exception exception) when (
                    attempt < 2 && IsTransientFailure(exception, cancellationToken))
                {
                    var retryDelay = TimeSpan.FromMilliseconds(50 * (1 << attempt));
                    Publish(Status with
                    {
                        State = TursoAutomaticSyncState.Retrying,
                        Attempt = attempt + 1,
                        LastException = exception,
                        NextAttempt = _timeProvider.GetUtcNow() + retryDelay,
                    });
                    await Task.Delay(retryDelay, _timeProvider, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    Publish(Status with
                    {
                        State = TursoAutomaticSyncState.Faulted,
                        Attempt = attempt + 1,
                        LastException = exception,
                        NextAttempt = null,
                    });
                    throw;
                }
            }
        }
    }

    private static bool IsTransientFailure(
        Exception exception,
        CancellationToken cancellationToken)
    {
        while (exception is TursoSyncException { InnerException: { } innerException })
            exception = innerException;

        return !cancellationToken.IsCancellationRequested
               && exception is HttpRequestException
                   or IOException
                   or TimeoutException
                   or OperationCanceledException;
    }

    private void PublishStopped()
    {
        Publish(Status with
        {
            State = TursoAutomaticSyncState.Stopped,
            Attempt = 0,
            NextAttempt = null,
        });
    }

    private void Publish(TursoAutomaticSyncStatus status)
    {
        Volatile.Write(ref _status, status);
        var handlers = StatusChanged;
        if (Volatile.Read(ref _disposed) != 0 || handlers is null)
            return;

        var args = new TursoAutomaticSyncStatusChangedEventArgs(status);
        _notifications.Enqueue(new Notification(args, handlers));
        ScheduleNotificationDrain();
    }

    private void ScheduleNotificationDrain()
    {
        if (Interlocked.Exchange(ref _notificationDrainScheduled, 1) != 0)
            return;

        ThreadPool.UnsafeQueueUserWorkItem(
            static coordinator => coordinator.DrainNotifications(),
            this,
            preferLocal: false);
    }

    private void DrainNotifications()
    {
        try
        {
            while (_notifications.TryDequeue(out var notification))
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    while (_notifications.TryDequeue(out _))
                    {
                    }
                    return;
                }

                foreach (var callback in notification.Handlers.GetInvocationList())
                {
                    try
                    {
                        ((EventHandler<TursoAutomaticSyncStatusChangedEventArgs>)callback)(
                            this,
                            notification.Args);
                    }
                    catch
                    {
                        // Observers cannot stop synchronization.
                    }
                }
            }
        }
        finally
        {
            Volatile.Write(ref _notificationDrainScheduled, 0);
            if (!_notifications.IsEmpty)
                ScheduleNotificationDrain();
        }
    }

    private sealed record Notification(
        TursoAutomaticSyncStatusChangedEventArgs Args,
        EventHandler<TursoAutomaticSyncStatusChangedEventArgs> Handlers);
}
