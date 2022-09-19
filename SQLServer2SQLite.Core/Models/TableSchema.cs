using System.Collections.Generic;

namespace SQLServer2SQLite.Core.Models
{
    public class TableSchema
    {
        public string TableName { get; set; }

        public string TableSchemaName { get; set; }

        public List<ColumnSchema> Columns { get; set; }

        public List<string> PrimaryKey { get; set; }

        public List<ForeignKeySchema> ForeignKeys { get; set; }

        public List<SchemaIndex> Indexes { get; set; }
    }
}
