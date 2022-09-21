using SqlServer2SqLite.Core.Models;

namespace SqlServer2SqLite.Tests.Builders;

public class TriggerBuilder
{
    private static ForeignKeySchema GetForeignKeySchema() =>
        new ForeignKeySchema()
        {
            TableName = "TableName2",
            ColumnName = "ColumnName2",
            ForeignTableName = "ForeignTableName1",
            ForeignColumnName = "ForeignColumnName1",
            CascadeOnDelete = true,
            IsNullable = true,
        };

    private static TableSchema GetTableSchema() =>
        new TableSchema()
        {
            TableName = "TableName1",
            TableSchemaName = "TableSchemaName1",
            Columns = new List<ColumnSchema>()
            {
                new ColumnSchema()
                {
                    ColumnName = "ColumnName1",
                    ColumnType = "ColumnType1",
                    Length = 2,
                    IsNullable = true,
                    DefaultValue = "10",
                    IsIdentity = true,
                    IsCaseSensitivite = true,
                },
                new ColumnSchema()
                {
                    ColumnName = "ColumnName2",
                    ColumnType = "ColumnType2",
                    Length = 5,
                    IsNullable = false,
                    DefaultValue = null,
                    IsIdentity = false,
                    IsCaseSensitivite = false,
                }
            },
            PrimaryKey = new List<string>() { "PrimaryKey1", "PrimaryKey2", },
            ForeignKeys = new List<ForeignKeySchema>()
            {
                new ForeignKeySchema()
                {
                    TableName = "TableName2",
                    ColumnName = "ColumnName2",
                    ForeignTableName = "ForeignTableName1",
                    ForeignColumnName = "ForeignColumnName1",
                    CascadeOnDelete = true,
                    IsNullable = true,
                },
                new ForeignKeySchema()
                {
                    TableName = "TableName3",
                    ColumnName = "ColumnName3",
                    ForeignTableName = "ForeignTableName4",
                    ForeignColumnName = "ForeignColumnName4",
                    CascadeOnDelete = false,
                    IsNullable = false,
                }
            },
            Indexes = new List<SchemaIndex>()
            {
                new SchemaIndex()
                {
                    IndexName = "TableName2",
                    IsUnique = true,
                    Columns = new List<ColumnIndex>()
                    {
                        new ColumnIndex() { ColumnName = "ColumnName1", IsAscending = true },
                        new ColumnIndex() { ColumnName = "ColumnName2", IsAscending = false }
                    }
                },
                new SchemaIndex()
                {
                    IndexName = "TableName3",
                    IsUnique = false,
                    Columns = new List<ColumnIndex>()
                    {
                        new ColumnIndex() { ColumnName = "ColumnName4", IsAscending = true }
                    }
                }
            },
        };

    [Test]
    public void GetForeignKeyTriggers()
    {
        Assert.That(
            Core.Builders.TriggerBuilder.GetForeignKeyTriggers(GetTableSchema()).Count,
            Is.EqualTo(6)
        );
    }

    [Test]
    public void MakeTriggerName()
    {
        Assert.That(
            Core.Builders.TriggerBuilder.MakeTriggerName(GetForeignKeySchema(), "prefix"),
            Is.EqualTo("prefix_TableName2_ColumnName2_ForeignTableName1_ForeignColumnName1")
        );
    }

    [Test]
    public void GenerateInsertTrigger()
    {
        Assert.That(
            Core.Builders.TriggerBuilder.GenerateInsertTrigger(GetForeignKeySchema()).ToString(),
            Is.EqualTo(
                "fki_TableName2_ColumnName2_ForeignTableName1_ForeignColumnName1;Insert;Before;SELECT RAISE(ROLLBACK, 'insert on table TableName2 violates foreign key constraint fki_TableName2_ColumnName2_ForeignTableName1_ForeignColumnName1') WHERE NEW.ColumnName2 IS NOT NULL AND (SELECT ForeignColumnName1 FROM ForeignTableName1 WHERE ForeignColumnName1 = NEW.ColumnName2) IS NULL; ;TableName2;"
            )
        );
    }

    [Test]
    public void GenerateUpdateTrigger()
    {
        Assert.That(
            Core.Builders.TriggerBuilder.GenerateUpdateTrigger(GetForeignKeySchema()).ToString(),
            Is.EqualTo(
                "fku_TableName2_ColumnName2_ForeignTableName1_ForeignColumnName1;Update;Before;SELECT RAISE(ROLLBACK, 'update on table TableName2 violates foreign key constraint fku_TableName2_ColumnName2_ForeignTableName1_ForeignColumnName1') WHERE NEW.ColumnName2 IS NOT NULL AND (SELECT ForeignColumnName1 FROM ForeignTableName1 WHERE ForeignColumnName1 = NEW.ColumnName2) IS NULL; ;TableName2;"
            )
        );
    }

    [Test]
    public void GenerateDeleteTrigger()
    {
        Assert.That(
            Core.Builders.TriggerBuilder.GenerateDeleteTrigger(GetForeignKeySchema()).ToString(),
            Is.EqualTo(
                "fkd_TableName2_ColumnName2_ForeignTableName1_ForeignColumnName1;Delete;Before;DELETE FROM [TableName2] WHERE ColumnName2 = OLD.ForeignColumnName1; ;ForeignTableName1;"
            )
        );
    }
}
