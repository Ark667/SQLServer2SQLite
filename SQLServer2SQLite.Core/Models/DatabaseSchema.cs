using System.Collections.Generic;
using System.Linq;
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

        public TableSchema[] GetOrderedTables()
        {
            return AddDependentTables(Tables.Where(o => !o.ForeignKeys.Any()).ToList());
        }

        private TableSchema[] AddDependentTables(List<TableSchema> initialTables)
        {
            var dependentTables = Tables
                .Where(o =>
                {
                    var sourceTableNames = initialTables.Select(ft => ft.TableName).ToArray();
                    var foreignTableNames = o.ForeignKeys.Select(f => f.ForeignTableName).ToArray();

                    return !sourceTableNames.Contains(o.TableName)
                        && foreignTableNames.All(a => sourceTableNames.Contains(a));
                })
                .ToArray();

            initialTables.AddRange(dependentTables);

            // Recurse
            if (!Tables.All(a => initialTables.Contains(a)))
                return AddDependentTables(initialTables);
            else
                return initialTables.ToArray();
        }
    }
}
