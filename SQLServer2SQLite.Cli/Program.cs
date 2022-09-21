using CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SqlServer2SqLite.Cli.Options;
using System;

namespace SqlServer2SqLite.Cli;

internal class Program
{
    internal static IServiceProvider ServiceProvider { get; set; }

    /// <summary>
    /// The Main.
    /// </summary>
    /// <param name="args">The args<see cref="string[]"/>.</param>
    /// <returns>The <see cref="int"/>.</returns>
    public static int Main(string[] args)
    {
        try
        {
            // Configure services
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            ServiceProvider = serviceCollection.BuildServiceProvider();

            // Start command line parser
            return Parser.Default
                .ParseArguments<ConvertOptions>(args)
                .MapResult(
                    (ConvertOptions opts) => opts.Convert(),
                    errs =>
                    {
                        return 1;
                    }
                );
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            throw;
        }
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        services.AddLogging(configure => configure.AddConsole());
    }
}
