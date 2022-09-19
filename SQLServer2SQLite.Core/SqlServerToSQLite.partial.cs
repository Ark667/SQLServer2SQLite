using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using SQLServer2SQLite.Core.Builders;
using SQLServer2SQLite.Core.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;

namespace SQLServer2SQLite.Core
{
    public partial class SqlServerToSQLite
    {
        /// <summary>
        /// Used in order to adjust the value received from SQL Servr for the SQLite database.
        /// </summary>
        /// <param name="val">The value object</param>
        /// <param name="columnSchema">The corresponding column schema</param>
        /// <returns>SQLite adjusted value.</returns>
        private static object CastValueForColumn(object val, ColumnSchema columnSchema)
        {
            if (val is DBNull)
                return null;

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
                        return ParseStringAsGuid((string)val);
                    if (val is byte[])
                        return ParseBlobAsGuid((byte[])val);
                    break;

                case DbType.Binary:
                case DbType.Boolean:
                case DbType.DateTime:
                    break;

                default:
                    Logger.LogError("argument exception - illegal database type");
                    throw new ArgumentException(
                        "Illegal database type [" + Enum.GetName(typeof(DbType), dt) + "]"
                    );
            } // switch

            return val;
        }

        private static Guid ParseBlobAsGuid(byte[] blob)
        {
            byte[] data = blob;
            if (blob.Length > 16)
            {
                data = new byte[16];
                for (int i = 0; i < 16; i++)
                    data[i] = blob[i];
            }
            else if (blob.Length < 16)
            {
                data = new byte[16];
                for (int i = 0; i < blob.Length; i++)
                    data[i] = blob[i];
            }

            return new Guid(data);
        }

        private static Guid ParseStringAsGuid(string str)
        {
            try
            {
                return new Guid(str);
            }
            catch (Exception)
            {
                return Guid.Empty;
            }
        }

        /// <summary>
        /// Creates a command object needed to insert values into a specific SQLite table.
        /// </summary>
        /// <param name="ts">The table schema object for the table.</param>
        /// <returns>A command object with the required functionality.</returns>
        private static SqliteCommand BuildSQLiteInsert(TableSchema ts)
        {
            SqliteCommand res = new SqliteCommand();

            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO [" + ts.TableName + "] (");
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                sb.Append("[" + ts.Columns[i].ColumnName + "]");
                if (i < ts.Columns.Count - 1)
                    sb.Append(", ");
            }
            sb.Append(") VALUES (");

            List<string> pnames = new List<string>();
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                string pname = "@" + GetNormalizedName(ts.Columns[i].ColumnName, pnames);
                sb.Append(pname);
                if (i < ts.Columns.Count - 1)
                    sb.Append(", ");

                SqliteType dbType = GetSqLiteDbTypeOfColumn(ts.Columns[i]);
                SqliteParameter prm = new SqliteParameter(
                    pname,
                    dbType,
                    1,
                    ts.Columns[i].ColumnName
                );
                res.Parameters.Add(prm);

                // Remember the parameter name in order to avoid duplicates
                pnames.Add(pname);
            }
            sb.Append(")");
            res.CommandText = sb.ToString();
            res.CommandType = CommandType.Text;
            return res;
        }

        /// <summary>
        /// Used in order to avoid breaking naming rules (e.g., when a table has
        /// a name in SQL Server that cannot be used as a basis for a matching index
        /// name in SQLite).
        /// </summary>
        /// <param name="str">The name to change if necessary</param>
        /// <param name="names">Used to avoid duplicate names</param>
        /// <returns>A normalized name</returns>
        private static string GetNormalizedName(string str, List<string> names)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < str.Length; i++)
            {
                if (Char.IsLetterOrDigit(str[i]) || str[i] == '_')
                    sb.Append(str[i]);
                else
                    sb.Append("_");
            }

            // Avoid returning duplicate name
            if (names.Contains(sb.ToString()))
                return GetNormalizedName(sb.ToString() + "_", names);
            else
                return sb.ToString();
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

        private static SqliteType GetSqLiteDbTypeOfColumn(ColumnSchema cs)
        {
            if (cs.ColumnType == "tinyint")
                return SqliteType.Integer;
            if (cs.ColumnType == "int")
                return SqliteType.Integer;
            if (cs.ColumnType == "smallint")
                return SqliteType.Integer;
            if (cs.ColumnType == "bigint")
                return SqliteType.Integer;
            if (cs.ColumnType == "bit")
                return SqliteType.Integer;
            if (
                cs.ColumnType == "nvarchar"
                || cs.ColumnType == "varchar"
                || cs.ColumnType == "text"
                || cs.ColumnType == "ntext"
            )
                return SqliteType.Text;
            if (cs.ColumnType == "float")
                return SqliteType.Real;
            if (cs.ColumnType == "real")
                return SqliteType.Real;
            if (cs.ColumnType == "blob")
                return SqliteType.Blob;
            if (cs.ColumnType == "numeric")
                return SqliteType.Real;
            if (
                cs.ColumnType == "timestamp"
                || cs.ColumnType == "datetime"
                || cs.ColumnType == "datetime2"
                || cs.ColumnType == "date"
                || cs.ColumnType == "time"
                || cs.ColumnType == "datetimeoffset"
            )
                return SqliteType.Text;
            if (cs.ColumnType == "nchar" || cs.ColumnType == "char")
                return SqliteType.Text;
            if (cs.ColumnType == "uniqueidentifier" || cs.ColumnType == "guid")
                return SqliteType.Text;
            if (cs.ColumnType == "xml")
                return SqliteType.Text;
            if (cs.ColumnType == "sql_variant")
                return SqliteType.Text;
            if (cs.ColumnType == "integer")
                return SqliteType.Integer;

            Logger.LogError("illegal db type found");
            throw new ApplicationException("Illegal DB type found (" + cs.ColumnType + ")");
        }

        /// <summary>
        /// Builds a SELECT query for a specific table. Needed in the process of copying rows
        /// from the SQL Server database to the SQLite database.
        /// </summary>
        /// <param name="ts">The table schema of the table for which we need the query.</param>
        /// <returns>The SELECT query for the table.</returns>
        private static string BuildSqlServerTableQuery(TableSchema ts)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ");
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                sb.Append("[" + ts.Columns[i].ColumnName + "]");
                if (i < ts.Columns.Count - 1)
                    sb.Append(", ");
            }
            sb.Append(" FROM " + ts.TableSchemaName + "." + "[" + ts.TableName + "]");
            return sb.ToString();
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

        /// <summary>
        /// Creates the CREATE TABLE DDL for SQLite and a specific table.
        /// </summary>
        /// <param name="conn">The SQLite connection</param>
        /// <param name="dt">The table schema object for the table to be generated.</param>
        private static void AddSQLiteTable(SqliteConnection conn, TableSchema dt)
        {
            // Prepare a CREATE TABLE DDL statement
            string stmt = TableBuilder.BuildCreateTableQuery(dt);

            Logger.LogInformation("\n\n" + stmt + "\n\n");

            // Execute the query in order to actually create the table.
            SqliteCommand cmd = new SqliteCommand(stmt, conn);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Creates a TableSchema object using the specified SQL Server connection
        /// and the name of the table for which we need to create the schema.
        /// </summary>
        /// <param name="conn">The SQL Server connection to use</param>
        /// <param name="tableName">The name of the table for which we wants to create the table schema.</param>
        /// <returns>A table schema object that represents our knowledge of the table schema</returns>
        private static TableSchema CreateTableSchema(
            SqlConnection conn,
            string tableName,
            string tschma
        )
        {
            TableSchema res = new TableSchema();
            res.TableName = tableName;
            res.TableSchemaName = tschma;
            res.Columns = new List<ColumnSchema>();
            SqlCommand cmd = new SqlCommand(
                @"SELECT COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE, "
                    + @" (columnproperty(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity')) AS [IDENT], "
                    + @"CHARACTER_MAXIMUM_LENGTH AS CSIZE "
                    + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
                    + tableName
                    + "' ORDER BY "
                    + "ORDINAL_POSITION ASC",
                conn
            );
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    object tmp = reader["COLUMN_NAME"];
                    if (tmp is DBNull)
                        continue;
                    string colName = (string)reader["COLUMN_NAME"];

                    tmp = reader["COLUMN_DEFAULT"];
                    string colDefault;
                    if (tmp is DBNull)
                        colDefault = string.Empty;
                    else
                        colDefault = (string)tmp;

                    tmp = reader["IS_NULLABLE"];
                    bool isNullable = ((string)tmp == "YES");
                    string dataType = (string)reader["DATA_TYPE"];
                    bool isIdentity = false;
                    if (reader["IDENT"] != DBNull.Value)
                        isIdentity = ((int)reader["IDENT"]) == 1 ? true : false;
                    int length =
                        reader["CSIZE"] != DBNull.Value ? Convert.ToInt32(reader["CSIZE"]) : 0;

                    ValidateDataType(dataType);

                    // Note that not all data type names need to be converted because
                    // SQLite establishes type affinity by searching certain strings
                    // in the type name. For example - everything containing the string
                    // 'int' in its type name will be assigned an INTEGER affinity
                    if (dataType == "timestamp")
                        dataType = "blob";
                    else if (
                        dataType == "datetime"
                        || dataType == "smalldatetime"
                        || dataType == "date"
                        || dataType == "datetime2"
                        || dataType == "time"
                    )
                        dataType = "datetime";
                    else if (dataType == "decimal")
                        dataType = "numeric";
                    else if (dataType == "money" || dataType == "smallmoney")
                        dataType = "numeric";
                    else if (dataType == "binary" || dataType == "varbinary" || dataType == "image")
                        dataType = "blob";
                    else if (dataType == "tinyint")
                        dataType = "smallint";
                    else if (dataType == "bigint")
                        dataType = "integer";
                    else if (dataType == "sql_variant")
                        dataType = "blob";
                    else if (dataType == "xml")
                        dataType = "varchar";
                    else if (dataType == "uniqueidentifier")
                        dataType = "guid";
                    else if (dataType == "ntext")
                        dataType = "text";
                    else if (dataType == "nchar")
                        dataType = "char";

                    if (dataType == "bit" || dataType == "int")
                    {
                        if (colDefault == "('False')")
                            colDefault = "(0)";
                        else if (colDefault == "('True')")
                            colDefault = "(1)";
                    }

                    colDefault = FixDefaultValueString(colDefault);

                    ColumnSchema col = new ColumnSchema();
                    col.ColumnName = colName;
                    col.ColumnType = dataType;
                    col.Length = length;
                    col.IsNullable = isNullable;
                    col.IsIdentity = isIdentity;
                    col.DefaultValue = AdjustDefaultValue(colDefault);
                    res.Columns.Add(col);
                }
            }

            // Find PRIMARY KEY information
            SqlCommand cmd2 = new SqlCommand(@"EXEC sp_pkeys '" + tableName + "'", conn);
            using (SqlDataReader reader = cmd2.ExecuteReader())
            {
                res.PrimaryKey = new List<string>();
                while (reader.Read())
                {
                    string colName = (string)reader["COLUMN_NAME"];
                    res.PrimaryKey.Add(colName);
                }
            }

            // Find COLLATE information for all columns in the table
            SqlCommand cmd4 = new SqlCommand(
                @"EXEC sp_tablecollations '" + tschma + "." + tableName + "'",
                conn
            );
            using (SqlDataReader reader = cmd4.ExecuteReader())
            {
                while (reader.Read())
                {
                    bool? isCaseSensitive = null;
                    string colName = (string)reader["name"];
                    if (reader["tds_collation"] != DBNull.Value)
                    {
                        byte[] mask = (byte[])reader["tds_collation"];
                        if ((mask[2] & 0x10) != 0)
                            isCaseSensitive = false;
                        else
                            isCaseSensitive = true;
                    }

                    if (isCaseSensitive.HasValue)
                    {
                        // Update the corresponding column schema.
                        foreach (ColumnSchema csc in res.Columns)
                        {
                            if (csc.ColumnName == colName)
                            {
                                csc.IsCaseSensitivite = isCaseSensitive;
                                break;
                            }
                        }
                    }
                }
            }

            try
            {
                // Find index information
                SqlCommand cmd3 = new SqlCommand(
                    @"exec sp_helpindex '" + tschma + "." + tableName + "'",
                    conn
                );
                using (SqlDataReader reader = cmd3.ExecuteReader())
                {
                    res.Indexes = new List<SchemaIndex>();
                    while (reader.Read())
                    {
                        string indexName = (string)reader["index_name"];
                        string desc = (string)reader["index_description"];
                        string keys = (string)reader["index_keys"];

                        // Don't add the index if it is actually a primary key index
                        if (desc.Contains("primary key"))
                            continue;

                        SchemaIndex index = BuildIndexSchema(indexName, desc, keys);
                        res.Indexes.Add(index);
                    }
                }
            }
            catch (Exception)
            {
                Logger.LogWarning("failed to read index information for table [" + tableName + "]");
            }

            return res;
        }

        /// <summary>
        /// Small validation method to make sure we don't miss anything without getting
        /// an exception.
        /// </summary>
        /// <param name="dataType">The datatype to validate.</param>
        private static void ValidateDataType(string dataType)
        {
            if (
                dataType == "int"
                || dataType == "smallint"
                || dataType == "bit"
                || dataType == "float"
                || dataType == "real"
                || dataType == "nvarchar"
                || dataType == "varchar"
                || dataType == "timestamp"
                || dataType == "varbinary"
                || dataType == "image"
                || dataType == "text"
                || dataType == "ntext"
                || dataType == "bigint"
                || dataType == "char"
                || dataType == "numeric"
                || dataType == "binary"
                || dataType == "smalldatetime"
                || dataType == "smallmoney"
                || dataType == "money"
                || dataType == "tinyint"
                || dataType == "uniqueidentifier"
                || dataType == "xml"
                || dataType == "sql_variant"
                || dataType == "datetime2"
                || dataType == "date"
                || dataType == "time"
                || dataType == "decimal"
                || dataType == "nchar"
                || dataType == "datetime"
                || dataType == "datetimeoffset"
            )
                return;
            throw new ApplicationException("Validation failed for data type [" + dataType + "]");
        }

        /// <summary>
        /// Does some necessary adjustments to a value string that appears in a column DEFAULT
        /// clause.
        /// </summary>
        /// <param name="colDefault">The original default value string (as read from SQL Server).</param>
        /// <returns>Adjusted DEFAULT value string (for SQLite)</returns>
        private static string FixDefaultValueString(string colDefault)
        {
            bool replaced = false;
            string res = colDefault.Trim();

            // Find first/last indexes in which to search
            int first = -1;
            int last = -1;
            for (int i = 0; i < res.Length; i++)
            {
                if (res[i] == '\'' && first == -1)
                    first = i;
                if (res[i] == '\'' && first != -1 && i > last)
                    last = i;
            }

            if (first != -1 && last > first)
                return res.Substring(first, last - first + 1);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < res.Length; i++)
            {
                if (res[i] != '(' && res[i] != ')')
                {
                    sb.Append(res[i]);
                    replaced = true;
                }
            }
            if (replaced)
                return "(" + sb.ToString() + ")";
            else
                return sb.ToString();
        }

        /// <summary>
        /// Add foreign key schema object from the specified components (Read from SQL Server).
        /// </summary>
        /// <param name="conn">The SQL Server connection to use</param>
        /// <param name="ts">The table schema to whom foreign key schema should be added to</param>
        private static void CreateForeignKeySchema(SqlConnection conn, TableSchema ts)
        {
            ts.ForeignKeys = new List<ForeignKeySchema>();

            SqlCommand cmd = new SqlCommand(
                @"SELECT "
                    + @"  ColumnName = CU.COLUMN_NAME, "
                    + @"  ForeignTableName  = PK.TABLE_NAME, "
                    + @"  ForeignColumnName = PT.COLUMN_NAME, "
                    + @"  DeleteRule = C.DELETE_RULE, "
                    + @"  IsNullable = COL.IS_NULLABLE "
                    + @"FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C "
                    + @"INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME "
                    + @"INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME "
                    + @"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME "
                    + @"INNER JOIN "
                    + @"  ( "
                    + @"    SELECT i1.TABLE_NAME, i2.COLUMN_NAME "
                    + @"    FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1 "
                    + @"    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME "
                    + @"    WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                    + @"  ) "
                    + @"PT ON PT.TABLE_NAME = PK.TABLE_NAME "
                    + @"INNER JOIN INFORMATION_SCHEMA.COLUMNS AS COL ON CU.COLUMN_NAME = COL.COLUMN_NAME AND FK.TABLE_NAME = COL.TABLE_NAME "
                    + @"WHERE FK.Table_NAME='"
                    + ts.TableName
                    + "'",
                conn
            );

            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    ForeignKeySchema fkc = new ForeignKeySchema();
                    fkc.ColumnName = (string)reader["ColumnName"];
                    fkc.ForeignTableName = (string)reader["ForeignTableName"];
                    fkc.ForeignColumnName = (string)reader["ForeignColumnName"];
                    fkc.CascadeOnDelete = (string)reader["DeleteRule"] == "CASCADE";
                    fkc.IsNullable = (string)reader["IsNullable"] == "YES";
                    fkc.TableName = ts.TableName;
                    ts.ForeignKeys.Add(fkc);
                }
            }
        }

        /// <summary>
        /// Builds an index schema object from the specified components (Read from SQL Server).
        /// </summary>
        /// <param name="indexName">The name of the index</param>
        /// <param name="desc">The description of the index</param>
        /// <param name="keys">Key columns that are part of the index.</param>
        /// <returns>An index schema object that represents our knowledge of the index</returns>
        private static SchemaIndex BuildIndexSchema(string indexName, string desc, string keys)
        {
            SchemaIndex res = new SchemaIndex();
            res.IndexName = indexName;

            // Determine if this is a unique index or not.
            string[] descParts = desc.Split(',');
            foreach (string p in descParts)
            {
                if (p.Trim().Contains("unique"))
                {
                    res.IsUnique = true;
                    break;
                }
            }

            // Get all key names and check if they are ASCENDING or DESCENDING
            res.Columns = new List<ColumnIndex>();
            string[] keysParts = keys.Split(',');
            foreach (string p in keysParts)
            {
                Match m = _keyRx.Match(p.Trim());
                if (!m.Success)
                {
                    throw new ApplicationException(
                        "Illegal key name [" + p + "] in index [" + indexName + "]"
                    );
                }

                string key = m.Groups[1].Value;
                ColumnIndex ic = new ColumnIndex();
                ic.ColumnName = key;
                if (m.Groups[2].Success)
                    ic.IsAscending = false;
                else
                    ic.IsAscending = true;

                res.Columns.Add(ic);
            }

            return res;
        }

        /// <summary>
        /// More adjustments for the DEFAULT value clause.
        /// </summary>
        /// <param name="val">The value to adjust</param>
        /// <returns>Adjusted DEFAULT value string</returns>
        private static string AdjustDefaultValue(string val)
        {
            if (val == null || val == string.Empty)
                return val;

            Match m = _defaultValueRx.Match(val);
            if (m.Success)
                return m.Groups[1].Value;
            return val;
        }

        /// <summary>
        /// Creates SQLite connection string from the specified DB file path.
        /// </summary>
        /// <param name="sqlitePath">The path to the SQLite database file.</param>
        /// <returns>SQLite connection string</returns>
        private static string CreateSQLiteConnectionString(string sqlitePath, string password)
        {
            SqliteConnectionStringBuilder builder = new SqliteConnectionStringBuilder();
            builder.DataSource = sqlitePath;
            if (password != null)
                builder.Password = password;
            //builder.PageSize = 4096;
            //builder.UseUTF16Encoding = true;
            string connstring = builder.ConnectionString;

            return connstring;
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
        /// Gets a create script for the triggerSchema in sqlite syntax
        /// </summary>
        /// <param name="ts">Trigger to script</param>
        /// <returns>Executable script</returns>
        public static string WriteTriggerSchema(TriggerSchema ts)
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

        private static Regex _keyRx = new Regex(@"(([a-zA-Z_äöüÄÖÜß0-9\.]|(\s+))+)(\(\-\))?");
        private static Regex _defaultValueRx = new Regex(@"\(N(\'.*\')\)");
    }
}
