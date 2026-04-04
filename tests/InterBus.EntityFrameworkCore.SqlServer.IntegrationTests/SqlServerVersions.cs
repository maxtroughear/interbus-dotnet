namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public class SqlServerVersions
{
    public static string[] All =>
    [
        "mcr.microsoft.com/mssql/server:2022-latest",
        "mcr.microsoft.com/mssql/server:2025-latest"
    ];
}
