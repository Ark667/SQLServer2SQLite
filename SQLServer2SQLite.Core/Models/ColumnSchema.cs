using Microsoft.Data.Sqlite;
using System;

namespace SqlServer2SqLite.Core.Models;

public class ColumnSchema
{
    public string ColumnName { get; set; }

    public string ColumnType { get; set; }

    public int Length { get; set; }

    public bool IsNullable { get; set; }

    public string DefaultValue { get; set; }

    public bool IsIdentity { get; set; }

    public bool? IsCaseSensitivite { get; set; }

    public string SqLiteColumnType
    {
        get
        {
            switch (GetSqLiteDbTypeOfColumn(ColumnType))
            {
                case SqliteType.Integer:
                    return "INTEGER";
                case SqliteType.Real:
                    return "REAL";
                case SqliteType.Text:
                    return "TEXT";
                case SqliteType.Blob:
                    return "BLOB";
                default:
                    return null;
            }
        }
    }

    // TODO test!
    public static SqliteType GetSqLiteDbTypeOfColumn(string columnType)
    {
        if (columnType == "tinyint")
            return SqliteType.Integer;
        if (columnType == "int")
            return SqliteType.Integer;
        if (columnType == "smallint")
            return SqliteType.Integer;
        if (columnType == "bigint")
            return SqliteType.Integer;
        if (columnType == "bit")
            return SqliteType.Integer;
        if (
            columnType == "nvarchar"
            || columnType == "varchar"
            || columnType == "text"
            || columnType == "ntext"
        )
            return SqliteType.Text;
        if (columnType == "float")
            return SqliteType.Real;
        if (columnType == "real")
            return SqliteType.Real;
        if (columnType == "blob")
            return SqliteType.Blob;
        if (columnType == "numeric")
            return SqliteType.Real;
        if (
            columnType == "timestamp"
            || columnType == "datetime"
            || columnType == "datetime2"
            || columnType == "date"
            || columnType == "time"
            || columnType == "datetimeoffset"
        )
            return SqliteType.Text;
        if (columnType == "nchar" || columnType == "char")
            return SqliteType.Text;
        if (columnType == "uniqueidentifier" || columnType == "guid")
            return SqliteType.Text;
        if (columnType == "xml")
            return SqliteType.Text;
        if (columnType == "sql_variant")
            return SqliteType.Text;
        if (columnType == "integer")
            return SqliteType.Integer;

        throw new InvalidOperationException("Illegal DB type found (" + columnType + ")");
    }
}
