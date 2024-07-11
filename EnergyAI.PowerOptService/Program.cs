using EnergyAI.PowerOptService;

IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService()
    .UseSystemd()
    .ConfigureServices(services =>
    {
        services.AddHostedService<Worker>();
    })
    .Build();

if (IsRunAgain()) return;
await host.RunAsync();

bool IsRunAgain()
{
    var processName = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name;
    System.Diagnostics.Process[] processes = System.Diagnostics.Process.GetProcessesByName(processName);
    return processes.Length > 1;
}
