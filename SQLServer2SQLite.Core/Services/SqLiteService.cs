using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using Microsoft.Data.SqlClient;
using SqlServer2SqLite.Core.Models;
using Microsoft.Extensions.Logging;
using SqlServer2SqLite.Core.Builders;
using System.Data;
using System.Text;
using System.IO;
using System.Linq;
using SQLServer2SQLite.Core.Builders;
using SQLServer2SQLite.Core.Helpers;
using SqlServer2SqLite.Core.Helpers;

namespace SqlServer2SqLite.Core.Services
{
    public class SqLiteService
    {
        private static ILogger<SqLiteService> Logger { get; set; }

        public SqLiteService(ILogger<SqLiteService> logger)
        {
            Logger = logger;
        }

        /// <summary>
        /// Creates the SQLite database from the schema read from the SQL Server.
        /// </summary>
        /// <param name="sqlitePath">The path to the generated DB file.</param>
        /// <param name="schema">The schema of the SQL server database.</param>
        /// <param name="password">The password to use for encrypting the DB or null if non is needed.</param>
        public void CreateSqLiteDatabase(
            string sqlitePath,
            DatabaseSchema schema,
            string password = null
        )
        {
            // Delete the target file if it exists already.
            if (File.Exists(sqlitePath))
                File.Delete(sqlitePath);

            // Connect to the newly created database
            string sqliteConnString = ConnectionStringHelper.CreateSQLiteConnectionString(
                sqlitePath,
                password
            );
            using (SqliteConnection conn = new SqliteConnection(sqliteConnString))
            {
                conn.Open();

                // Create all tables in the new database
                foreach (TableSchema dt in schema.Tables)
                {
                    try
                    {
                        // Prepare a CREATE TABLE DDL statement
                        string stmt = TableBuilder.BuildCreateTableQuery(dt);

                        Logger.LogInformation(stmt);

                        // Execute the query in order to actually create the table.
                        var cmd = new SqliteCommand(stmt, conn);
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError("AddSQLiteTable failed", ex);
                        throw;
                    }

                    Logger.LogInformation("added schema for SQLite table [" + dt.TableName + "]");
                }

                // Add triggers based on foreign key constraints
                AddTriggersForForeignKeys(sqlitePath, schema.Tables, password);
                Logger.LogInformation($"Source rows copied on target database");
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
            string sqlitePath,
            List<TableSchema> tableSchemas,
            string password
        )
        {
            // Connect to the SQLite database
            using (
                var sqlConnection = new SqliteConnection(
                    ConnectionStringHelper.CreateSQLiteConnectionString(sqlitePath, password)
                )
            )
            {
                sqlConnection.Open();
                SetConstraintCheck(sqlConnection, false);

                // Go over all tables in the schema and copy their rows
                foreach (var tableSchema in tableSchemas)
                {
                    var inserted = 0;
                    var query = InsertBuilder.BuildSQLiteInsert(tableSchema);
                    query.Connection = sqlConnection;

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
                                query.Parameters[pname].Value = CastValueForColumn(
                                    row[column.ColumnName],
                                    column
                                );
                                pnames.Add(pname);
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

                    SetConstraintCheck(sqlConnection, true);
                    Logger.LogInformation(
                        $"Inserted {inserted} rows for table [{tableSchema.TableName}]"
                    );
                }
            }
        }

        public void AddTriggersForForeignKeys(
            string sqlitePath,
            IEnumerable<TableSchema> schema,
            string password
        )
        {
            // Connect to the newly created database
            string sqliteConnString = ConnectionStringHelper.CreateSQLiteConnectionString(
                sqlitePath,
                password
            );
            using (SqliteConnection conn = new SqliteConnection(sqliteConnString))
            {
                conn.Open();

                foreach (TableSchema dt in schema)
                {
                    try
                    {
                        AddTableTriggers(conn, dt);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError("AddTableTriggers failed", ex);
                        throw;
                    }
                }
            }

            Logger.LogInformation("finished adding triggers to schema");
        }

        private static void AddSQLiteView(SqliteConnection conn, ViewSchema vs)
        {
            // Prepare a CREATE VIEW DDL statement
            string stmt = vs.ViewSQL;
            Logger.LogInformation("\n\n" + stmt + "\n\n");

            // Execute the query in order to actually create the view.
            SqliteTransaction tx = conn.BeginTransaction();
            try
            {
                SqliteCommand cmd = new SqliteCommand(stmt, conn, tx);
                cmd.ExecuteNonQuery();

                tx.Commit();
            }
            catch (SqliteException)
            {
                tx.Rollback();
            }
        }

        private static void AddTableTriggers(SqliteConnection conn, TableSchema dt)
        {
            IList<TriggerSchema> triggers = TriggerBuilder.GetForeignKeyTriggers(dt);
            foreach (TriggerSchema trigger in triggers)
            {
                SqliteCommand cmd = new SqliteCommand(WriteTriggerSchema(trigger), conn);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Used in order to adjust the value received from SQL Servr for the SQLite database.
        /// </summary>
        /// <param name="val">The value object</param>
        /// <param name="columnSchema">The corresponding column schema</param>
        /// <returns>SQLite adjusted value.</returns>
        private static object CastValueForColumn(object val, ColumnSchema columnSchema)
        {
            if (val is DBNull)
                return DBNull.Value;

            DbType dt = GetDbTypeOfColumn(columnSchema);

            switch (dt)
            {
                case DbType.Int32:
                    if (val is short)
                        return (int)(short)val;
                    if (val is byte)
                        return (int)(byte)val;
                    if (val is long)
                        return (int)(long)val;
                    if (val is decimal)
                        return (int)(decimal)val;
                    break;

                case DbType.Int16:
                    if (val is int)
                        return (short)(int)val;
                    if (val is byte)
                        return (short)(byte)val;
                    if (val is long)
                        return (short)(long)val;
                    if (val is decimal)
                        return (short)(decimal)val;
                    break;

                case DbType.Int64:
                    if (val is int)
                        return (long)(int)val;
                    if (val is short)
                        return (long)(short)val;
                    if (val is byte)
                        return (long)(byte)val;
                    if (val is decimal)
                        return (long)(decimal)val;
                    break;

                case DbType.Single:
                    if (val is double)
                        return (float)(double)val;
                    if (val is decimal)
                        return (float)(decimal)val;
                    break;

                case DbType.Double:
                    if (val is float)
                        return (double)(float)val;
                    if (val is double)
                        return (double)val;
                    if (val is decimal)
                        return (double)(decimal)val;
                    break;

                case DbType.String:
                    if (val is Guid)
                        return ((Guid)val).ToString();
                    break;

                case DbType.Guid:
                    if (val is string)
                        return TextHelper.ParseStringAsGuid((string)val);
                    if (val is byte[])
                        return TextHelper.ParseBlobAsGuid((byte[])val);
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

            return val;
        }

        /// <summary>
        /// Matches SQL Server types to general DB types
        /// </summary>
        /// <param name="cs">The column schema to use for the match</param>
        /// <returns>The matched DB type</returns>
        private static DbType GetDbTypeOfColumn(ColumnSchema cs)
        {
            if (cs.ColumnType == "tinyint")
                return DbType.Byte;
            if (cs.ColumnType == "int")
                return DbType.Int32;
            if (cs.ColumnType == "smallint")
                return DbType.Int16;
            if (cs.ColumnType == "bigint")
                return DbType.Int64;
            if (cs.ColumnType == "bit")
                return DbType.Boolean;
            if (
                cs.ColumnType == "nvarchar"
                || cs.ColumnType == "varchar"
                || cs.ColumnType == "text"
                || cs.ColumnType == "ntext"
            )
                return DbType.String;
            if (cs.ColumnType == "float")
                return DbType.Double;
            if (cs.ColumnType == "real")
                return DbType.Single;
            if (cs.ColumnType == "blob")
                return DbType.Binary;
            if (cs.ColumnType == "numeric")
                return DbType.Double;
            if (
                cs.ColumnType == "timestamp"
                || cs.ColumnType == "datetime"
                || cs.ColumnType == "datetime2"
                || cs.ColumnType == "date"
                || cs.ColumnType == "time"
                || cs.ColumnType == "datetimeoffset"
            )
                return DbType.DateTime;
            if (cs.ColumnType == "nchar" || cs.ColumnType == "char")
                return DbType.String;
            if (cs.ColumnType == "uniqueidentifier" || cs.ColumnType == "guid")
                return DbType.Guid;
            if (cs.ColumnType == "xml")
                return DbType.String;
            if (cs.ColumnType == "sql_variant")
                return DbType.Object;
            if (cs.ColumnType == "integer")
                return DbType.Int64;

            Logger.LogError("illegal db type found");
            throw new ApplicationException("Illegal DB type found (" + cs.ColumnType + ")");
        }

        /// <summary>
        /// Gets a create script for the triggerSchema in sqlite syntax
        /// </summary>
        /// <param name="ts">Trigger to script</param>
        /// <returns>Executable script</returns>
        private static string WriteTriggerSchema(TriggerSchema ts)
        {
            return @"CREATE TRIGGER ["
                + ts.Name
                + "] "
                + ts.Type
                + " "
                + ts.Event
                + " ON ["
                + ts.Table
                + "] "
                + "BEGIN "
                + ts.Body
                + " END;";
        }

        // TODO test!
        public static void SetConstraintCheck(SqliteConnection sqliteConnection, bool enabled)
        {
            var command = sqliteConnection.CreateCommand();
            command.CommandText = $"PRAGMA ignore_check_constraints = {(enabled ? 0 : 1)};";
            command.ExecuteNonQuery();
        }
    }
}
