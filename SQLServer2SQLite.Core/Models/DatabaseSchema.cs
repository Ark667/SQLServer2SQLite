using System.Collections.Generic;

namespace SQLServer2SQLite.Core.Models
{
    public class DatabaseSchema
    {
        public List<TableSchema> Tables { get; set; } = new List<TableSchema>();
        public List<ViewSchema> Views { get; set; } = new List<ViewSchema>();
    }
}
