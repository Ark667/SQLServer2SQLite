using System.Collections.Generic;

namespace SqlServer2SqLite.Core.Models
{
    public class SchemaIndex
    {
        public string IndexName { get; set; }

        public bool IsUnique { get; set; }

        public List<ColumnIndex> Columns { get; set; }
    }
}
