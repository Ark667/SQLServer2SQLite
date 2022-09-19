using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.IO;
using Microsoft.Data.Sqlite;
using Microsoft.Data.SqlClient;
using SQLServer2SQLite.Core.Models;
using Microsoft.Extensions.Logging;

namespace SQLServer2SQLite.Core
{
    /// <summary>
    /// This class is resposible to take a single SQL Server database
    /// and convert it to an SQLite database file.
    /// </summary>
    /// <remarks>The class knows how to convert table and index structures only.</remarks>
    public partial class SqlServerToSQLite
    {
        private static ILogger<SqlServerToSQLite> Logger { get; set; }

        public SqlServerToSQLite(ILogger<SqlServerToSQLite> logger)
        {
            Logger = logger;
        }

        /// <summary>
        /// Do the entire process of first reading the SQL Server schema, creating a corresponding
        /// SQLite schema, and copying all rows from the SQL Server database to the SQLite database.
        /// </summary>
        /// <param name="sqlConnString">The SQL Server connection string</param>
        /// <param name="sqlitePath">The path to the generated SQLite database file</param>
        /// <param name="password">The password to use or NULL if no password should be used to encrypt the DB</param>
        /// <param name="handler">A handler to handle progress notifications.</param>
        /// <param name="selectionHandler">The selection handler which allows the user to select which tables to
        /// convert.</param>
        public void ConvertSqlServerDatabaseToSQLiteFile(
            string sqlConnString,
            string sqlitePath,
            string password
        )
        {
            // Delete the target file if it exists already.
            if (File.Exists(sqlitePath))
                File.Delete(sqlitePath);

            // Read the schema of the SQL Server database into a memory structure
            DatabaseSchema ds = ReadSqlServerSchema(sqlConnString);

            // Create the SQLite database and apply the schema
            CreateSqLiteDatabase(sqlitePath, ds, password);

            // Copy all rows from SQL Server tables to the newly created SQLite database
            CopySqlServerRowsToSqLiteDatabase(sqlConnString, sqlitePath, ds.Tables, password);

            // Add triggers based on foreign key constraints
            AddTriggersForForeignKeys(sqlitePath, ds.Tables, password);
        }

        /// <summary>
        /// Reads the entire SQL Server DB schema using the specified connection string.
        /// </summary>
        /// <param name="connString">The connection string used for reading SQL Server schema.</param>
        ///
        ///
        /// <returns>database schema objects for every table/view in the SQL Server database.</returns>
        private static DatabaseSchema ReadSqlServerSchema(string connString)
        {
            // First step is to read the names of all tables in the database
            List<TableSchema> tables = new List<TableSchema>();
            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();

                List<string> tableNames = new List<string>();
                List<string> tblschema = new List<string>();

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

                    Logger.LogDebug("parsed table schema for [" + tname + "]");
                }
            }

            Logger.LogDebug("finished parsing all tables in SQL Server schema");

            Regex removedbo = new Regex(@"dbo\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // Continue and read all of the views in the database
            List<ViewSchema> views = new List<ViewSchema>();
            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();

                SqlCommand cmd = new SqlCommand(
                    @"SELECT TABLE_NAME, VIEW_DEFINITION  from INFORMATION_SCHEMA.VIEWS",
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

                        Logger.LogDebug("parsed view schema for [" + vs.ViewName + "]");
                    }
                }
            }

            DatabaseSchema ds = new DatabaseSchema();
            ds.Tables = tables;
            ds.Views = views;
            return ds;
        }

        /// <summary>
        /// Creates the SQLite database from the schema read from the SQL Server.
        /// </summary>
        /// <param name="sqlitePath">The path to the generated DB file.</param>
        /// <param name="schema">The schema of the SQL server database.</param>
        /// <param name="password">The password to use for encrypting the DB or null if non is needed.</param>
        private static void CreateSqLiteDatabase(
            string sqlitePath,
            DatabaseSchema schema,
            string password = null
        )
        {
            // Connect to the newly created database
            string sqliteConnString = CreateSQLiteConnectionString(sqlitePath, password);
            using (SqliteConnection conn = new SqliteConnection(sqliteConnString))
            {
                conn.Open();

                // Create all tables in the new database
                foreach (TableSchema dt in schema.Tables)
                {
                    try
                    {
                        AddSQLiteTable(conn, dt);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError("AddSQLiteTable failed", ex);
                        throw;
                    }

                    Logger.LogDebug("added schema for SQLite table [" + dt.TableName + "]");
                }
            }

            Logger.LogDebug("finished adding all table/view schemas for SQLite database");
        }

        /// <summary>
        /// Copies table rows from the SQL Server database to the SQLite database.
        /// </summary>
        /// <param name="sqlConnString">The SQL Server connection string</param>
        /// <param name="sqlitePath">The path to the SQLite database file.</param>
        /// <param name="schemas">The schema of the SQL Server database.</param>
        /// <param name="password">The password to use for encrypting the file</param>
        private static void CopySqlServerRowsToSqLiteDatabase(
            string sqlConnString,
            string sqlitePath,
            List<TableSchema> schemas,
            string password
        )
        {
            Logger.LogDebug("preparing to insert tables ...");

            // Connect to the SQL Server database
            using (SqlConnection ssconn = new SqlConnection(sqlConnString))
            {
                ssconn.Open();

                // Connect to the SQLite database next
                string sqliteConnString = CreateSQLiteConnectionString(sqlitePath, password);
                using (SqliteConnection sqconn = new SqliteConnection(sqliteConnString))
                {
                    sqconn.Open();

                    // Go over all tables in the schema and copy their rows
                    foreach (var schema in schemas)
                    {
                        SqliteTransaction tx = sqconn.BeginTransaction();
                        try
                        {
                            string tableQuery = BuildSqlServerTableQuery(schema);
                            var query = new SqlCommand(tableQuery, ssconn);

                            using (SqlDataReader reader = query.ExecuteReader())
                            {
                                SqliteCommand insert = BuildSQLiteInsert(schema);
                                int counter = 0;
                                while (reader.Read())
                                {
                                    insert.Connection = sqconn;
                                    insert.Transaction = tx;
                                    List<string> pnames = new List<string>();
                                    for (
                                        int columnIndex = 0;
                                        columnIndex < schema.Columns.Count;
                                        columnIndex++
                                    )
                                    {
                                        var pname =
                                            $"@{GetNormalizedName(schema.Columns[columnIndex].ColumnName, pnames)}";
                                        insert.Parameters[pname].Value = CastValueForColumn(
                                            reader[columnIndex],
                                            schema.Columns[columnIndex]
                                        );
                                        pnames.Add(pname);
                                    }
                                    insert.ExecuteNonQuery();
                                    counter++;
                                    if (counter % 1000 == 0)
                                    {
                                        tx.Commit();
                                        tx = sqconn.BeginTransaction();
                                    }
                                }
                            }

                            tx.Commit();

                            Logger.LogDebug(
                                "finished inserting all rows for table [" + schema.TableName + "]"
                            );
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError("unexpected exception", ex);
                            tx.Rollback();
                            throw;
                        }
                    }
                }
            }
        }

        private static void AddTriggersForForeignKeys(
            string sqlitePath,
            IEnumerable<TableSchema> schema,
            string password
        )
        {
            // Connect to the newly created database
            string sqliteConnString = CreateSQLiteConnectionString(sqlitePath, password);
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

            Logger.LogDebug("finished adding triggers to schema");
        }
    }
}
