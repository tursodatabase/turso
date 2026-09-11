using System.Data.Common;

namespace Turso;

public sealed class TursoBatchCommandCollection : DbBatchCommandCollection
{
    private readonly List<TursoBatchCommand> _commands = [];

    public override int Count => _commands.Count;

    public override bool IsReadOnly => false;

    public override void Add(DbBatchCommand item)
    {
        _commands.Add(RequireTursoBatchCommand(item));
    }

    public override void Clear()
    {
        _commands.Clear();
    }

    public override bool Contains(DbBatchCommand item)
    {
        return item is TursoBatchCommand command && _commands.Contains(command);
    }

    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        for (var i = 0; i < _commands.Count; i++)
            array[arrayIndex + i] = _commands[i];
    }

    public override IEnumerator<DbBatchCommand> GetEnumerator()
    {
        return _commands.GetEnumerator();
    }

    public override int IndexOf(DbBatchCommand item)
    {
        return item is TursoBatchCommand command ? _commands.IndexOf(command) : -1;
    }

    public override void Insert(int index, DbBatchCommand item)
    {
        _commands.Insert(index, RequireTursoBatchCommand(item));
    }

    public override bool Remove(DbBatchCommand item)
    {
        return item is TursoBatchCommand command && _commands.Remove(command);
    }

    public override void RemoveAt(int index)
    {
        _commands.RemoveAt(index);
    }

    protected override DbBatchCommand GetBatchCommand(int index)
    {
        return _commands[index];
    }

    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand)
    {
        _commands[index] = RequireTursoBatchCommand(batchCommand);
    }

    internal IReadOnlyList<TursoBatchCommand> AsReadOnly()
    {
        return _commands;
    }

    private static TursoBatchCommand RequireTursoBatchCommand(DbBatchCommand command)
    {
        return command as TursoBatchCommand
               ?? throw new ArgumentException("Batch command must be a TursoBatchCommand.", nameof(command));
    }
}
