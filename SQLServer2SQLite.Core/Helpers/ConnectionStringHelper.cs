using Microsoft.Data.Sqlite;

namespace SqlServer2SqLite.Core.Helpers
{
    public static class ConnectionStringHelper
    {
        public static string GetSqlServerConnectionString1(
            string sourceHost,
            string sourceDatabase,
            string sourceUsername,
            string sourcePassword
        )
        {
            if (string.IsNullOrEmpty(sourceUsername) && string.IsNullOrEmpty(sourcePassword))
                return GetSqlServerConnectionString(sourceHost, sourceDatabase);
            else
                return GetSqlServerConnectionString(
                    sourceHost,
                    sourceDatabase,
                    sourceUsername,
                    sourcePassword
                );
        }

        public static string GetSqlServerConnectionString(string address, string db)
        {
            return @"Data Source="
                + address.Trim()
                + ";Initial Catalog="
                + db.Trim()
                + ";Integrated Security=SSPI;";
        }

        public static string GetSqlServerConnectionString(
            string address,
            string db,
            string user,
            string pass
        )
        {
            return @"Data Source="
                + address.Trim()
                + ";Initial Catalog="
                + db.Trim()
                + ";User ID="
                + user.Trim()
                + ";Password="
                + pass.Trim();
        }

        public static string CreateSQLiteConnectionString(string sqlitePath, string password)
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
    }
}
