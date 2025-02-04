using Microsoft.Extensions.Logging;
using SqlServer2SqLite.Core.Services;

namespace SqlServer2SqLite.Core;

/// <summary>
/// This class is resposible to take a single SQL Server database and convert it to an SQLite database file.
/// </summary>
public class SqlServerToSQLite
{
    public ILogger<SqlServerToSQLite> Logger { get; }
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
    /// Read SQL Server schema and create a corresponding SQLite schema.
    /// </summary>
    public void Create()
    {
        // Read the schema of the SQL Server database into a memory structure
        var databaseSchema = SqlServerService.GetDatabaseSchema();
        Logger.LogInformation($"Source schema loaded");

        // Create the SQLite database and apply the schema
        SqLiteService.CreateSqLiteDatabase(databaseSchema);
        Logger.LogInformation($"Target schema created");
    }

    /// <summary>
    /// Read SQL Server schema and copy all rows from the SQL Server database to the SQLite database.
    /// </summary>
    public void Copy()
    {
        // Read the schema of the SQL Server database into a memory structure
        var databaseSchema = SqlServerService.GetDatabaseSchema();
        Logger.LogInformation($"Source schema loaded");

        // Read all rows from the SQL Server database
        var rows = SqlServerService.GetDatabaseRows(databaseSchema.Tables.ToArray());
        Logger.LogInformation($"Rows loaded from source database");

        // Copy all rows from SQL Server tables to the newly created SQLite database
        SqLiteService.CopySqlServerRowsToSqLiteDatabase(rows, databaseSchema.GetOrderedTables());
        Logger.LogInformation($"Rows copied on target database");
    }
}
