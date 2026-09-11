namespace Turso.Serverless.Client;

/// <summary>Encodes statement arguments into the protocol's positional/named parameter shape.</summary>
internal static class SqlArgs
{
    internal static HranaStatement ToStatement(string sql, IReadOnlyList<object?>? args, IReadOnlyDictionary<string, object?>? namedArgs, bool wantRows)
    {
        var statement = new HranaStatement { Sql = sql, WantRows = wantRows };

        if (args is not null)
        {
            statement.Args = args.Select(HranaValue.Encode).ToList();
        }

        if (namedArgs is not null)
        {
            statement.NamedArgs = namedArgs
                .Select(static pair => new HranaNamedArg { Name = pair.Key, Value = HranaValue.Encode(pair.Value) })
                .ToList();
        }

        return statement;
    }
}
