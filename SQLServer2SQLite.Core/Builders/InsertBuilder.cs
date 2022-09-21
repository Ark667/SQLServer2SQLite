using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using SqlServer2SqLite.Core.Models;
using SQLServer2SQLite.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SQLServer2SQLite.Core.Builders
{
    public class InsertBuilder
    {
        /// <summary>
        /// Creates a command object needed to insert values into a specific SQLite table.
        /// </summary>
        /// <param name="tableSchema">The table schema object for the table.</param>
        /// <returns>A command object with the required functionality.</returns>
        // TODO test!
        public static SqliteCommand BuildSQLiteInsert(TableSchema tableSchema)
        {
            SqliteCommand res = new SqliteCommand();

            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO [" + tableSchema.TableName + "] (");
            for (int i = 0; i < tableSchema.Columns.Count; i++)
            {
                sb.Append("[" + tableSchema.Columns[i].ColumnName + "]");
                if (i < tableSchema.Columns.Count - 1)
                    sb.Append(", ");
            }
            sb.Append(") VALUES (");

            List<string> pnames = new List<string>();
            for (int i = 0; i < tableSchema.Columns.Count; i++)
            {
                string pname =
                    "@" + TextHelper.GetNormalizedName(tableSchema.Columns[i].ColumnName, pnames);
                sb.Append(pname);
                if (i < tableSchema.Columns.Count - 1)
                    sb.Append(", ");

                SqliteType dbType = GetSqLiteDbTypeOfColumn(tableSchema.Columns[i]);
                SqliteParameter prm = new SqliteParameter(
                    pname,
                    dbType,
                    1,
                    tableSchema.Columns[i].ColumnName
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

        // TODO test!
        public static SqliteType GetSqLiteDbTypeOfColumn(ColumnSchema cs)
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

            throw new InvalidOperationException("Illegal DB type found (" + cs.ColumnType + ")");
        }
    }
}
