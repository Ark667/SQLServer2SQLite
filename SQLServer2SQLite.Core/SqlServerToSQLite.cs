using Microsoft.Extensions.Logging;
using SqlServer2SqLite.Core.Services;

namespace SqlServer2SqLite.Core
{
    /// <summary>
    /// This class is resposible to take a single SQL Server database
    /// and convert it to an SQLite database file.
    /// </summary>
    public partial class SqlServerToSQLite
    {
        private static ILogger<SqlServerToSQLite> Logger { get; set; }
        public SqlServerService SqlServerService { get; }
        public SqLiteService SqLiteService { get; }

        public SqlServerToSQLite(
            ILogger<SqlServerToSQLite> logger,
            SqlServerService sqlServerService,
            SqLiteService sqLiteService
        )
        {
            Logger = logger;
            SqlServerService = sqlServerService;
            SqLiteService = sqLiteService;
        }

        /// <summary>
        /// Do the entire process of first reading the SQL Server schema, creating a corresponding
        /// SQLite schema, and copying all rows from the SQL Server database to the SQLite database.
        /// </summary>
        /// <param name="sourceConnectionString">The SQL Server connection string</param>
        /// <param name="targetFilePath">The path to the generated SQLite database file</param>
        /// <param name="password">The password to use or NULL if no password should be used to encrypt the DB</param>
        public void Convert(string sourceConnectionString, string targetFilePath, string password)
        {
            // Read the schema of the SQL Server database into a memory structure
            var databaseSchema = SqlServerService.GetDatabaseSchema(sourceConnectionString);
            Logger.LogInformation($"Source schema loaded");

            // Create the SQLite database and apply the schema
            SqLiteService.CreateSqLiteDatabase(targetFilePath, databaseSchema, password);
            Logger.LogInformation($"Target schema created on {targetFilePath}");

            // Read all rows from the SQL Server database
            var rows = SqlServerService.GetDatabaseRows(
                sourceConnectionString,
                databaseSchema.Tables
            );
            Logger.LogInformation($"Rows loaded from source database");

            // Copy all rows from SQL Server tables to the newly created SQLite database
            SqLiteService.CopySqlServerRowsToSqLiteDatabase(
                rows,
                targetFilePath,
                databaseSchema.Tables,
                password
            );
            Logger.LogInformation($"Rows copied on target database");
        }
    }
}
