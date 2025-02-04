using Microsoft.Data.Sqlite;

namespace SqlServer2SqLite.Core.Helpers;

public static class ConnectionStringHelper
{
    public static string GetSqlServerConnectionString(
        string sourceHost,
        string sourceDatabase,
        string sourceUsername,
        string sourcePassword
    )
    {
        if (string.IsNullOrEmpty(sourceUsername) || string.IsNullOrEmpty(sourcePassword))
            return @"Data Source="
                + sourceHost.Trim()
                + ";Initial Catalog="
                + sourceDatabase.Trim()
                + ";Integrated Security=SSPI;";
        else
            return @"Data Source="
                + sourceHost.Trim()
                + ";Initial Catalog="
                + sourceDatabase.Trim()
                + ";User ID="
                + sourceUsername.Trim()
                + ";Password="
                + sourcePassword.Trim();
    }

    public static string CreateSQLiteConnectionString(string sqlitePath, string password)
    {
        var builder = new SqliteConnectionStringBuilder();
        builder.DataSource = sqlitePath;
        if (!string.IsNullOrEmpty(password))
            builder.Password = password;
        //builder.PageSize = 4096;
        //builder.UseUTF16Encoding = true;

        return builder.ConnectionString;
    }
}
