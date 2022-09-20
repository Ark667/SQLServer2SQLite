using SQLServer2SQLite.Core.Models;
using System.Text;
using System.Text.RegularExpressions;

namespace SQLServer2SQLite.Core.Builders
{
    public static class TableBuilder
    {
        /// <summary>
        /// returns the CREATE TABLE DDL for creating the SQLite table from the specified
        /// table schema object.
        /// </summary>
        /// <param name="ts">The table schema object from which to create the SQL statement.</param>
        /// <returns>CREATE TABLE DDL for the specified table.</returns>
        public static string BuildCreateTableQuery(TableSchema ts)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("CREATE TABLE [" + ts.TableName + "] (\n");

            bool pkey = false;
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                ColumnSchema col = ts.Columns[i];
                string cline = BuildColumnStatement(col, ts, ref pkey);
                sb.Append(cline);
                if (i < ts.Columns.Count - 1)
                    sb.Append(",\n");
            }

            // add primary keys...
            if (ts.PrimaryKey != null && ts.PrimaryKey.Count > 0 & !pkey)
            {
                sb.Append(",\n");
                sb.Append("    PRIMARY KEY (");
                for (int i = 0; i < ts.PrimaryKey.Count; i++)
                {
                    sb.Append("[" + ts.PrimaryKey[i] + "]");
                    if (i < ts.PrimaryKey.Count - 1)
                        sb.Append(", ");
                }
                sb.Append(")\n");
            }
            else
                sb.Append("\n");

            // add foreign keys...
            if (ts.ForeignKeys.Count > 0)
            {
                sb.Append(",\n");
                for (int i = 0; i < ts.ForeignKeys.Count; i++)
                {
                    ForeignKeySchema foreignKey = ts.ForeignKeys[i];
                    string stmt = string.Format(
                        "    FOREIGN KEY ([{0}])\n        REFERENCES [{1}]([{2}])",
                        foreignKey.ColumnName,
                        foreignKey.ForeignTableName,
                        foreignKey.ForeignColumnName
                    );

                    sb.Append(stmt);
                    if (i < ts.ForeignKeys.Count - 1)
                        sb.Append(",\n");
                }
            }

            sb.Append("\n");
            sb.Append(");\n");

            // Create any relevant indexes
            if (ts.Indexes != null)
            {
                for (int i = 0; i < ts.Indexes.Count; i++)
                {
                    string stmt = BuildCreateIndex(ts.TableName, ts.Indexes[i]);
                    sb.Append(stmt + ";\n");
                }
            }

            string query = sb.ToString();
            return query;
        }

        /// <summary>
        /// Used when creating the CREATE TABLE DDL. Creates a single row
        /// for the specified column.
        /// </summary>
        /// <param name="col">The column schema</param>
        /// <returns>A single column line to be inserted into the general CREATE TABLE DDL statement</returns>
        public static string BuildColumnStatement(ColumnSchema col, TableSchema ts, ref bool pkey)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("\t[" + col.ColumnName + "]\t");

            // Special treatment for IDENTITY columns
            if (col.IsIdentity)
            {
                if (
                    ts.PrimaryKey.Count == 1
                    && (
                        col.ColumnType == "tinyint"
                        || col.ColumnType == "int"
                        || col.ColumnType == "smallint"
                        || col.ColumnType == "bigint"
                        || col.ColumnType == "integer"
                    )
                )
                {
                    sb.Append("integer PRIMARY KEY AUTOINCREMENT");
                    pkey = true;
                }
                else
                    sb.Append("integer");
            }
            else
            {
                if (col.ColumnType == "int")
                    sb.Append("integer");
                else
                {
                    sb.Append(col.ColumnType);
                }
                if (col.Length > 0)
                    sb.Append("(" + col.Length + ")");
            }
            if (!col.IsNullable)
                sb.Append(" NOT NULL");

            if (col.IsCaseSensitivite.HasValue && !col.IsCaseSensitivite.Value)
                sb.Append(" COLLATE NOCASE");

            string defval = StripParens(col.DefaultValue);
            defval = DiscardNational(defval);
            //_log.Debug("DEFAULT VALUE BEFORE [" + col.DefaultValue + "] AFTER [" + defval + "]");
            if (!string.IsNullOrEmpty(defval) && defval.ToUpper().Contains("GETDATE"))
            {
                //_log.Debug(
                //    "converted SQL Server GETDATE() to CURRENT_TIMESTAMP for column ["
                //        + col.ColumnName
                //        + "]"
                //);
                sb.Append(" DEFAULT (CURRENT_TIMESTAMP)");
            }
            else if (!string.IsNullOrEmpty(defval) && IsValidDefaultValue(defval))
                sb.Append(" DEFAULT " + defval);

            return sb.ToString();
        }

        /// <summary>
        /// Creates a CREATE INDEX DDL for the specified table and index schema.
        /// </summary>
        /// <param name="tableName">The name of the indexed table.</param>
        /// <param name="indexSchema">The schema of the index object</param>
        /// <returns>A CREATE INDEX DDL (SQLite format).</returns>
        public static string BuildCreateIndex(string tableName, SchemaIndex indexSchema)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("CREATE ");
            if (indexSchema.IsUnique)
                sb.Append("UNIQUE ");
            sb.Append("INDEX [" + tableName + "_" + indexSchema.IndexName + "]\n");
            sb.Append("ON [" + tableName + "]\n");
            sb.Append("(");
            for (int i = 0; i < indexSchema.Columns.Count; i++)
            {
                sb.Append("[" + indexSchema.Columns[i].ColumnName + "]");
                if (!indexSchema.Columns[i].IsAscending)
                    sb.Append(" DESC");
                if (i < indexSchema.Columns.Count - 1)
                    sb.Append(", ");
            }
            sb.Append(")");

            return sb.ToString();
        }

        /// <summary>
        /// Discards the national prefix if exists (e.g., N'sometext') which is not
        /// supported in SQLite.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public static string DiscardNational(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;

            Regex rx = new Regex(@"N\'([^\']*)\'");
            Match m = rx.Match(value);
            if (m.Success)
                return m.Groups[1].Value;
            else
                return value;
        }

        /// <summary>
        /// Check if the DEFAULT clause is valid by SQLite standards
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool IsValidDefaultValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return false;

            if (IsSingleQuoted(value))
                return true;

            double testnum;
            if (!double.TryParse(value, out testnum))
                return false;

            return true;
        }

        /// <summary>
        /// ?
        /// </summary>
        /// <param name="value">?</param>
        /// <returns>?</returns>
        public static bool IsSingleQuoted(string value)
        {
            if (string.IsNullOrEmpty(value)) return false;

            value = value.Trim();
            if (value.StartsWith("'") && value.EndsWith("'"))
                return true;
            return false;
        }

        /// <summary>
        /// Strip any parentheses from the string.
        /// </summary>
        /// <param name="value">The string to strip</param>
        /// <returns>The stripped string</returns>
        public static string StripParens(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;

            Regex rx = new Regex(@"\(([^\)]*)\)");
            Match m = rx.Match(value);
            if (!m.Success)
                return value;
            else
                return StripParens(m.Groups[1].Value);
        }
    }
}
