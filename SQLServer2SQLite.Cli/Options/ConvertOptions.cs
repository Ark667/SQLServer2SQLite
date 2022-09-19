﻿using CommandLine;
using SQLServer2SQLite.Core.Helpers;
using SQLServer2SQLite.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace SQLServer2SQLite.Cli.Options;

/// <summary>
/// Defines the <see cref="ConvertOptions" />.
/// </summary>
[Verb("convert", HelpText = "Convert source SQLServer database to SQLite.")]
public class ConvertOptions
{
    /// <summary>
    /// Gets or sets the SourceHost.
    /// </summary>
    [Option('h', "sourceHost", HelpText = "Host name with SQLServer database.", Required = true)]
    public string SourceHost { get; set; }

    /// <summary>
    /// Gets or sets the SourceDatabase.
    /// </summary>
    [Option('d', "sourceDatabase", HelpText = "SQLServer database name.", Required = true)]
    public string SourceDatabase { get; set; }

    /// <summary>
    /// Gets or sets the SourceUsername.
    /// </summary>
    [Option(
        'u',
        "sourceUsername",
        HelpText = "Username to connect on database. If not set integrated security will be used."
    )]
    public string SourceUsername { get; set; }

    /// <summary>
    /// Gets or sets the SourcePassword.
    /// </summary>
    [Option(
        'p',
        "sourcePassword",
        HelpText = "Password to connect on database. If not set integrated security will be used."
    )]
    public string SourcePassword { get; set; }

    /// <summary>
    /// Gets or sets the TargetFile.
    /// </summary>
    [Option(
        't',
        "targetFile",
        HelpText = "File containing the converted SQLite database.",
        Required = true
    )]
    public string TargetFile { get; set; }

    /// <summary>
    /// Gets or sets the TargetPassword.
    /// </summary>
    [Option(
        'w',
        "targetPassword",
        HelpText = "Password to encrypt SQLite. If not set the file will open to public."
    )]
    public string TargetPassword { get; set; }

    /// <summary>
    /// The Convert.
    /// </summary>
    /// <returns>The <see cref="int"/>Zero if successful.</returns>
    public int Convert()
    {
        new SqlServerToSQLite(
            Program.ServiceProvider.GetRequiredService<ILogger<SqlServerToSQLite>>()
        ).ConvertSqlServerDatabaseToSQLiteFile(
            ConnectionStringHelper.GetSqlServerConnectionString1(
                SourceHost,
                SourceDatabase,
                SourceUsername,
                SourcePassword
            ),
            TargetFile,
            TargetPassword
        );

        return 1;
    }
}