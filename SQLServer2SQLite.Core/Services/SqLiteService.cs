using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using SqlServer2SqLite.Core.Models;
using Microsoft.Extensions.Logging;
using SqlServer2SqLite.Core.Builders;
using System.Data;
using System.Linq;
using SQLServer2SQLite.Core.Builders;
using SQLServer2SQLite.Core.Helpers;

namespace SqlServer2SqLite.Core.Services;

public class SqLiteService
{
    private ILogger<SqLiteService> Logger { get; set; }

    public SqLiteService(ILogger<SqLiteService> logger, string connectionString)
    {
        Logger = logger;
        ConnectionString = connectionString;
    }

    public string ConnectionString { get; set; }

    /// <summary>
    /// Creates the SQLite database from the schema read from the SQL Server.
    /// </summary>
    /// <param name="sqlitePath">The path to the generated DB file.</param>
    /// <param name="databaseSchema">The schema of the SQL server database.</param>
    /// <param name="password">The password to use for encrypting the DB or null if non is needed.</param>
    public void CreateSqLiteDatabase(DatabaseSchema databaseSchema)
    {
        using (var connection = new SqliteConnection(ConnectionString))
        {
            connection.Open();
            Logger.LogInformation($"Connected with {ConnectionString}");

            foreach (TableSchema tableSchema in databaseSchema.Tables)
            {
                // Prepare a CREATE TABLE DDL statement
                string query = TableBuilder.BuildCreateTableQuery(tableSchema);
                Logger.LogInformation(query);

                // Execute the query in order to actually create the table.
                var command = new SqliteCommand(query, connection);
                command.ExecuteNonQuery();
                Logger.LogInformation(
                    "added schema for SQLite table [" + tableSchema.TableName + "]"
                );
            }

            // Add triggers based on foreign key constraints
            CreateSqLiteTriggers(databaseSchema.Tables.ToArray());
            Logger.LogInformation($"Triggers created on target database");

            // Add triggers based on foreign key constraints
            // TODO AddSQLiteView(connectionString, databaseSchema.Views.ToArray());
            Logger.LogInformation($"Views created on target database");
        }

        Logger.LogInformation("finished adding all table/view schemas for SQLite database");
    }

    /// <summary>
    /// Copies table rows from the SQL Server database to the SQLite database.
    /// </summary>
    /// <param name="sqlConnString">The SQL Server connection string</param>
    /// <param name="sqlitePath">The path to the SQLite database file.</param>
    /// <param name="schemas">The schema of the SQL Server database.</param>
    /// <param name="password">The password to use for encrypting the file</param>
    public void CopySqlServerRowsToSqLiteDatabase(
        DataTable[] dataTables,
        TableSchema[] tableSchemas
    )
    {
        using (var connection = new SqliteConnection(ConnectionString))
        {
            connection.Open();
            Logger.LogInformation($"Connected with {ConnectionString}");

            // Go over all tables in the schema and copy their rows
            foreach (var tableSchema in tableSchemas)
            {
                var inserted = 0;
                var query = InsertBuilder.BuildSQLiteInsert(tableSchema);
                query.Connection = connection;

                var rows = dataTables
                    .First(o => o.TableName == tableSchema.TableName)
                    .Rows.OfType<DataRow>()
                    .ToArray();
                try
                {
                    foreach (var row in rows)
                    {
                        var pnames = new List<string>();
                        foreach (var column in tableSchema.Columns)
                        {
                            var pname =
                                $"@{TextHelper.GetNormalizedName(column.ColumnName, pnames)}";
                            var value = CastValueForColumn(row[column.ColumnName], column);
                            query.Parameters[pname].Value = value;
                            query.Parameters[pname].Size = value.ToString().Length;
                        }
                        query.ExecuteNonQuery();
                        inserted += 1;
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogError(
                        ex,
                        $"Failed inserting {query.CommandText} with {string.Join(", ", query.Parameters.Cast<SqliteParameter>().Select(o => $"{o.ParameterName}:{o.Value}"))}"
                    );
                }

                Logger.LogInformation(
                    $"Inserted {inserted} rows for table [{tableSchema.TableName}]"
                );
            }
        }
    }

    private void CreateSqLiteTriggers(TableSchema[] tableSchemas)
    {
        // Connect to the newly created database
        using (var connection = new SqliteConnection(ConnectionString))
        {
            connection.Open();

            foreach (TableSchema tableSchema in tableSchemas)
            {
                var triggers = TriggerBuilder.GetForeignKeyTriggers(tableSchema);
                foreach (TriggerSchema trigger in triggers)
                {
                    // Prepare a TRIGGER VIEW DDL statement
                    string query = trigger.ToString();
                    Logger.LogInformation("\n\n" + query + "\n\n");

                    SqliteCommand cmd = new SqliteCommand(query, connection);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        Logger.LogInformation("finished adding triggers to schema");
    }

    private void CreateSqLiteViews(ViewSchema[] viewSchemas)
    {
        using (var connection = new SqliteConnection(ConnectionString))
        {
            connection.Open();

            foreach (var viewSchema in viewSchemas)
            {
                // Prepare a CREATE VIEW DDL statement
                string stmt = viewSchema.ViewSQL;
                Logger.LogInformation("\n\n" + stmt + "\n\n");

                // Execute the query in order to actually create the view.
                SqliteCommand cmd = new SqliteCommand(stmt, connection);
                cmd.ExecuteNonQuery();
            }
        }

        Logger.LogInformation("finished adding views to schema");
    }

    /// <summary>
    /// Used in order to adjust the value received from SQL Servr for the SQLite database.
    /// </summary>
    /// <param name="value">The value object</param>
    /// <param name="columnSchema">The corresponding column schema</param>
    /// <returns>SQLite adjusted value.</returns>
    private static object CastValueForColumn(object value, ColumnSchema columnSchema)
    {
        if (value is DBNull)
            return DBNull.Value;

        DbType dt = GetDbTypeOfColumn(columnSchema);

        switch (dt)
        {
            case DbType.Int32:
                if (value is short)
                    return (int)(short)value;
                if (value is byte)
                    return (int)(byte)value;
                if (value is long)
                    return (int)(long)value;
                if (value is decimal)
                    return (int)(decimal)value;
                break;

            case DbType.Int16:
                if (value is int)
                    return (short)(int)value;
                if (value is byte)
                    return (short)(byte)value;
                if (value is long)
                    return (short)(long)value;
                if (value is decimal)
                    return (short)(decimal)value;
                break;

            case DbType.Int64:
                if (value is int)
                    return (long)(int)value;
                if (value is short)
                    return (long)(short)value;
                if (value is byte)
                    return (long)(byte)value;
                if (value is decimal)
                    return (long)(decimal)value;
                break;

            case DbType.Single:
                if (value is double)
                    return (float)(double)value;
                if (value is decimal)
                    return (float)(decimal)value;
                break;

            case DbType.Double:
                if (value is float)
                    return (double)(float)value;
                if (value is double)
                    return (double)value;
                if (value is decimal)
                    return (double)(decimal)value;
                break;

            case DbType.String:
                if (value is Guid)
                    return ((Guid)value).ToString();
                break;

            case DbType.Guid:
                if (value is string)
                    return TextHelper.ParseStringAsGuid((string)value);
                if (value is byte[])
                    return TextHelper.ParseBlobAsGuid((byte[])value);
                break;

            case DbType.Binary:
            case DbType.Boolean:
            case DbType.DateTime:
                break;

            default:
                throw new ArgumentException(
                    "Illegal database type [" + Enum.GetName(typeof(DbType), dt) + "]"
                );
        }

        return value;
    }

    /// <summary>
    /// Matches SQL Server types to general DB types
    /// </summary>
    /// <param name="columnSchema">The column schema to use for the match</param>
    /// <returns>The matched DB type</returns>
    private static DbType GetDbTypeOfColumn(ColumnSchema columnSchema)
    {
        if (columnSchema.ColumnType == "tinyint")
            return DbType.Byte;
        if (columnSchema.ColumnType == "int")
            return DbType.Int32;
        if (columnSchema.ColumnType == "smallint")
            return DbType.Int16;
        if (columnSchema.ColumnType == "bigint")
            return DbType.Int64;
        if (columnSchema.ColumnType == "bit")
            return DbType.Boolean;
        if (
            columnSchema.ColumnType == "nvarchar"
            || columnSchema.ColumnType == "varchar"
            || columnSchema.ColumnType == "text"
            || columnSchema.ColumnType == "ntext"
        )
            return DbType.String;
        if (columnSchema.ColumnType == "float")
            return DbType.Double;
        if (columnSchema.ColumnType == "real")
            return DbType.Single;
        if (columnSchema.ColumnType == "blob")
            return DbType.Binary;
        if (columnSchema.ColumnType == "numeric")
            return DbType.Double;
        if (
            columnSchema.ColumnType == "timestamp"
            || columnSchema.ColumnType == "datetime"
            || columnSchema.ColumnType == "datetime2"
            || columnSchema.ColumnType == "date"
            || columnSchema.ColumnType == "time"
            || columnSchema.ColumnType == "datetimeoffset"
        )
            return DbType.DateTime;
        if (columnSchema.ColumnType == "nchar" || columnSchema.ColumnType == "char")
            return DbType.String;
        if (columnSchema.ColumnType == "uniqueidentifier" || columnSchema.ColumnType == "guid")
            return DbType.Guid;
        if (columnSchema.ColumnType == "xml")
            return DbType.String;
        if (columnSchema.ColumnType == "sql_variant")
            return DbType.Object;
        if (columnSchema.ColumnType == "integer")
            return DbType.Int64;

        throw new InvalidOperationException(
            "Illegal DB type found (" + columnSchema.ColumnType + ")"
        );
    }
}
