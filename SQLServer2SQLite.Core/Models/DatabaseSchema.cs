using System.Collections.Generic;
using System.Text.Json;

namespace SqlServer2SqLite.Core.Models
{
    public class DatabaseSchema
    {
        public List<TableSchema> Tables { get; set; } = new List<TableSchema>();
        public List<ViewSchema> Views { get; set; } = new List<ViewSchema>();

        public override string ToString()
        {
            return JsonSerializer.Serialize(this);
        }
    }
}
