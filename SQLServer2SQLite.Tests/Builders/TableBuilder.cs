using SqlServer2SqLite.Core.Models;

namespace SqlServer2SqLite.Tests.Builders;

public class TableBuilder
{
    private static ColumnSchema GetColumnSchema() =>
        new ColumnSchema()
        {
            ColumnName = "ColumnName1",
            ColumnType = "ColumnType1",
            Length = 2,
            IsNullable = true,
            DefaultValue = "10",
            IsIdentity = true,
            IsCaseSensitivite = true,
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
                    ColumnType = "int",
                    Length = 2,
                    IsNullable = true,
                    DefaultValue = "10",
                    IsIdentity = true,
                    IsCaseSensitivite = true,
                },
                new ColumnSchema()
                {
                    ColumnName = "ColumnName2",
                    ColumnType = "varchar",
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
    public void BuildCreateTableQuery()
    {
        Assert.That(
            Core.Builders.TableBuilder.BuildCreateTableQuery(GetTableSchema()),
            Is.EqualTo(
                "CREATE TABLE [TableName1] (\n\t[ColumnName1]\tinteger DEFAULT 10,\n\t[ColumnName2]\tTEXT(5) NOT NULL COLLATE NOCASE,\n    PRIMARY KEY ([PrimaryKey1], [PrimaryKey2])\n,\n    FOREIGN KEY ([ColumnName2])\n        REFERENCES [ForeignTableName1]([ForeignColumnName1]),\n    FOREIGN KEY ([ColumnName3])\n        REFERENCES [ForeignTableName4]([ForeignColumnName4])\n);\nCREATE UNIQUE INDEX [TableName1_TableName2]\nON [TableName1]\n([ColumnName1], [ColumnName2] DESC);\nCREATE INDEX [TableName1_TableName3]\nON [TableName1]\n([ColumnName4]);\n"
            )
        );
    }

    [Test]
    public void BuildColumnStatement()
    {
        var primaryKey = true;

        Assert.That(
            Core.Builders.TableBuilder.BuildColumnStatement(
                GetColumnSchema(),
                GetTableSchema(),
                ref primaryKey
            ),
            Is.EqualTo("\t[ColumnName1]\tinteger DEFAULT 10")
        );
        Assert.That(!primaryKey, Is.False);
    }

    [Test]
    public void BuildCreateIndex()
    {
        Assert.That(
            Core.Builders.TableBuilder.BuildCreateIndex(
                "TableName1",
                new SchemaIndex()
                {
                    IndexName = "TableName2",
                    IsUnique = true,
                    Columns = new List<ColumnIndex>()
                    {
                        new ColumnIndex() { ColumnName = "ColumnName1", IsAscending = true },
                        new ColumnIndex() { ColumnName = "ColumnName2", IsAscending = false }
                    }
                }
            ),
            Is.EqualTo(
                "CREATE UNIQUE INDEX [TableName1_TableName2]\nON [TableName1]\n([ColumnName1], [ColumnName2] DESC)"
            )
        );
    }

    [Test]
    public void DiscardNational()
    {
        Assert.That(
            Core.Builders.TableBuilder.DiscardNational("N'sometext'"),
            Is.EqualTo("sometext")
        );
        Assert.That(
            Core.Builders.TableBuilder.DiscardNational("'sometext'"),
            Is.EqualTo("'sometext'")
        );
        Assert.That(
            Core.Builders.TableBuilder.DiscardNational("N'sometext"),
            Is.EqualTo("N'sometext")
        );
        Assert.That(Core.Builders.TableBuilder.DiscardNational(null), Is.EqualTo(null));
    }

    [Test]
    public void IsValidDefaultValue()
    {
        Assert.That(Core.Builders.TableBuilder.IsValidDefaultValue("'666'"), Is.True);
        Assert.That(Core.Builders.TableBuilder.IsValidDefaultValue("666"), Is.True);
        Assert.That(Core.Builders.TableBuilder.IsValidDefaultValue("aaa"), Is.False);
        Assert.That(Core.Builders.TableBuilder.IsValidDefaultValue(null), Is.False);
    }

    [Test]
    public void IsSingleQuoted()
    {
        Assert.That(Core.Builders.TableBuilder.IsSingleQuoted("'666'"), Is.True);
        Assert.That(Core.Builders.TableBuilder.IsSingleQuoted("666'"), Is.False);
        Assert.That(Core.Builders.TableBuilder.IsSingleQuoted("''666'"), Is.True);
        Assert.That(Core.Builders.TableBuilder.IsSingleQuoted("666"), Is.False);
        Assert.That(Core.Builders.TableBuilder.IsSingleQuoted(null), Is.False);
    }

    [Test]
    public void StripParens()
    {
        Assert.That(Core.Builders.TableBuilder.StripParens("(666)"), Is.EqualTo("666"));
        Assert.That(Core.Builders.TableBuilder.StripParens("(666"), Is.EqualTo("(666"));
        Assert.That(Core.Builders.TableBuilder.StripParens("666)"), Is.EqualTo("666)"));
        Assert.That(Core.Builders.TableBuilder.StripParens("666"), Is.EqualTo("666"));
        Assert.That(Core.Builders.TableBuilder.StripParens(null), Is.EqualTo(null));
    }
}
