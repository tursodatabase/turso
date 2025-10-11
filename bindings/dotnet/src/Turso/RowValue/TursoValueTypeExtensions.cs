using System.ComponentModel;

namespace Turso.RowValue;

public static class TursoValueTypeExtensions
{
    public static string GetTypeName(this TursoValueType valueType)
    {
        return valueType switch
        {
            TursoValueType.Empty => "",
            TursoValueType.Null => "NULL",
            TursoValueType.Integer => "INTEGER",
            TursoValueType.Real => "REAL",
            TursoValueType.Text => "TEXT",
            TursoValueType.Blob => "BLOB",
            _ => throw new InvalidEnumArgumentException(nameof(valueType))
        };
    }
}