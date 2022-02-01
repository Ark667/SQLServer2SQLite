using DbAccess;
using System;
using System.Collections.Generic;

namespace SQLServer2SQLite.Cli
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var host = "p2zu24z7dc.database.windows.net";
            var database = "db_ftptransport";
            var user = "Aingeru";
            var pass = "Medrano.2015";

            SqlConversionHandler handler = new SqlConversionHandler(delegate (bool done,
                bool success, int percent, string msg) {
                    
                });
            SqlTableSelectionHandler selectionHandler = new SqlTableSelectionHandler(delegate (List<TableSchema> schema)
            {
                List<TableSchema> updated = null;

                return updated;
            });

            FailedViewDefinitionHandler viewFailureHandler = new FailedViewDefinitionHandler(delegate (ViewSchema vs)
            {
                string updated = null;

                return updated;
            });

            string sqlConnString;
            if (false)
            {
                sqlConnString = GetSqlServerConnectionString(host, database);
            }
            else
            {
                sqlConnString = GetSqlServerConnectionString(host, database, user, pass);
            }

            SqlServerToSQLite.ConvertSqlServerToSQLiteDatabase(sqlConnString, "./mhctransport.db", null, null,
                null, null, false, false);
        }

        private static string GetSqlServerConnectionString(string address, string db)
        {
            string res = @"Data Source=" + address.Trim() +
                    ";Initial Catalog=" + db.Trim() + ";Integrated Security=SSPI;";
            return res;
        }
        private static string GetSqlServerConnectionString(string address, string db, string user, string pass)
        {
            string res = @"Data Source=" + address.Trim() +
                ";Initial Catalog=" + db.Trim() + ";User ID=" + user.Trim() + ";Password=" + pass.Trim();
            return res;
        }
    }
}
