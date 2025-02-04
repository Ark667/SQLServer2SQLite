using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using SqlServer2SqLite.Core.Models;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Data;

namespace SqlServer2SqLite.Core.Services;

public class SqlServerService
{
    private static Regex _keyRx = new Regex(@"(([a-zA-Z_äöüÄÖÜß0-9\.]|(\s+))+)(\(\-\))?");
    private static Regex _defaultValueRx = new Regex(@"\(N(\'.*\')\)");

    private ILogger<SqlServerService> Logger { get; }
    public string ConnectionString { get; }

    public SqlServerService(ILogger<SqlServerService> logger, string connectionString)
    {
        Logger = logger;
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Reads the entire SQL Server DB schema using the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string used for reading SQL Server schema.</param>
    /// <returns>database schema objects for every table/view in the SQL Server database.</returns>
    public DatabaseSchema GetDatabaseSchema()
    {
        // First step is to read the names of all tables in the database
        List<TableSchema> tables = new List<TableSchema>();
        using (SqlConnection conn = new SqlConnection(ConnectionString))
        {
            conn.Open();
            Logger.LogInformation($"Connected with {ConnectionString}");

            var tableNames = new List<string>();
            var tblschema = new List<string>();

            // This command will read the names of all tables in the database
            SqlCommand cmd = new SqlCommand(
                @"select * from INFORMATION_SCHEMA.TABLES  where TABLE_TYPE = 'BASE TABLE'",
                conn
            );
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    if (reader["TABLE_NAME"] == DBNull.Value)
                        continue;
                    if (reader["TABLE_SCHEMA"] == DBNull.Value)
                        continue;
                    tableNames.Add((string)reader["TABLE_NAME"]);
                    tblschema.Add((string)reader["TABLE_SCHEMA"]);
                }
            }

            // Next step is to use ADO APIs to query the schema of each table.
            int count = 0;
            for (int i = 0; i < tableNames.Count; i++)
            {
                string tname = tableNames[i];
                string tschma = tblschema[i];
                TableSchema ts = CreateTableSchema(conn, tname, tschma);
                CreateForeignKeySchema(conn, ts);
                tables.Add(ts);
                count++;

                Logger.LogInformation("parsed table schema for [" + tname + "]");
            }
        }

        Logger.LogInformation("finished parsing all tables in SQL Server schema");

        Regex removedbo = new Regex(@"dbo\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        // Continue and read all of the views in the database
        List<ViewSchema> views = new List<ViewSchema>();
        using (SqlConnection conn = new SqlConnection(ConnectionString))
        {
            conn.Open();

            SqlCommand cmd = new SqlCommand(
                @"select TABLE_NAME, VIEW_DEFINITION from INFORMATION_SCHEMA.VIEWS",
                conn
            );
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                int count = 0;
                while (reader.Read())
                {
                    ViewSchema vs = new ViewSchema();

                    if (reader["TABLE_NAME"] == DBNull.Value)
                        continue;
                    if (reader["VIEW_DEFINITION"] == DBNull.Value)
                        continue;
                    vs.ViewName = (string)reader["TABLE_NAME"];
                    vs.ViewSQL = (string)reader["VIEW_DEFINITION"];

                    // Remove all ".dbo" strings from the view definition
                    vs.ViewSQL = removedbo.Replace(vs.ViewSQL, string.Empty);

                    views.Add(vs);

                    count++;

                    Logger.LogInformation("parsed view schema for [" + vs.ViewName + "]");
                }
            }
        }

        return new DatabaseSchema { Tables = tables, Views = views };
    }

    /// <summary>
    /// Creates a TableSchema object using the specified SQL Server connection
    /// and the name of the table for which we need to create the schema.
    /// </summary>
    /// <param name="connection">The SQL Server connection to use</param>
    /// <param name="tableName">The name of the table for which we wants to create the table schema.</param>
    /// <returns>A table schema object that represents our knowledge of the table schema</returns>
    private static TableSchema CreateTableSchema(
        SqlConnection connection,
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
            connection
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
                bool isNullable = (string)tmp == "YES";
                string dataType = (string)reader["DATA_TYPE"];
                bool isIdentity = false;
                if (reader["IDENT"] != DBNull.Value)
                    isIdentity = (int)reader["IDENT"] == 1 ? true : false;
                int length = reader["CSIZE"] != DBNull.Value ? Convert.ToInt32(reader["CSIZE"]) : 0;

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
        SqlCommand cmd2 = new SqlCommand(@"EXEC sp_pkeys '" + tableName + "'", connection);
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
            connection
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

        // Find index information
        SqlCommand cmd3 = new SqlCommand(
            @"exec sp_helpindex '" + tschma + "." + tableName + "'",
            connection
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

        return res;
    }

    /// <summary>
    /// Add foreign key schema object from the specified components (Read from SQL Server).
    /// </summary>
    /// <param name="connection">The SQL Server connection to use</param>
    /// <param name="tableSchema">The table schema to whom foreign key schema should be added to</param>
    private static void CreateForeignKeySchema(SqlConnection connection, TableSchema tableSchema)
    {
        tableSchema.ForeignKeys = new List<ForeignKeySchema>();

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
                + tableSchema.TableName
                + "'",
            connection
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
                fkc.TableName = tableSchema.TableName;
                tableSchema.ForeignKeys.Add(fkc);
            }
        }
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
    /// <param name="value">The value to adjust</param>
    /// <returns>Adjusted DEFAULT value string</returns>
    private static string AdjustDefaultValue(string value)
    {
        if (value == null || value == string.Empty)
            return value;

        Match m = _defaultValueRx.Match(value);
        if (m.Success)
            return m.Groups[1].Value;
        return value;
    }

    /// <summary>
    /// Copies table rows from the SQL Server database to the SQLite database.
    /// </summary>
    /// <param name="tableSchemas">The schema of the SQL Server database.</param>
    ///
    public DataTable[] GetDatabaseRows(TableSchema[] tableSchemas)
    {
        var dataTables = new List<DataTable>();

        // Connect to the SQL Server database
        using (var sqlConnection = new SqlConnection(ConnectionString))
        {
            sqlConnection.Open();

            // Go over all tables in the schema and copy their rows
            foreach (var tableSchema in tableSchemas)
            {
                string query = BuildSqlServerTableQuery(tableSchema);
                var sqlCommand = new SqlCommand(query, sqlConnection);

                var dataTable = new DataTable(tableSchema.TableName);
                using (SqlDataReader reader = sqlCommand.ExecuteReader())
                    dataTable.Load(reader);

                dataTables.Add(dataTable);

                Logger.LogInformation(
                    $"Loaded {dataTable.Rows.Count} rows for table [{tableSchema.TableName}]"
                );
            }
        }

        return dataTables.ToArray();
    }

    private static string BuildSqlServerTableQuery(TableSchema tableSchema)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT ");
        for (int i = 0; i < tableSchema.Columns.Count; i++)
        {
            sb.Append("[" + tableSchema.Columns[i].ColumnName + "]");
            if (i < tableSchema.Columns.Count - 1)
                sb.Append(", ");
        }
        sb.Append($" FROM {tableSchema.TableSchemaName}.[{tableSchema.TableName}]");
        return sb.ToString();
    }
}
