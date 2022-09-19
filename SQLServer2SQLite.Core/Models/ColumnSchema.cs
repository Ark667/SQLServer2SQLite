namespace SQLServer2SQLite.Core.Models
{
    public class ColumnSchema
    {
        public string ColumnName { get; set; }

        public string ColumnType { get; set; }

        public int Length { get; set; }

        public bool IsNullable { get; set; }

        public string DefaultValue { get; set; }

        public bool IsIdentity { get; set; }

        public bool? IsCaseSensitivite { get; set; }
    }
}
