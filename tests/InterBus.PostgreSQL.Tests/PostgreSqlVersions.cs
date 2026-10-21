using System.Collections;

namespace InterBus.PostgreSQL.Tests;

public class PostgreSqlVersions
{
    public static IEnumerable All => Transactional.Concat(Lease);

    public static IEnumerable<TestFixtureData> Transactional
    {
        get
        {
            yield return new TestFixtureData("postgres:17", PostgreSqlMessagePumpMode.Transactional);
            yield return new TestFixtureData("postgres:18", PostgreSqlMessagePumpMode.Transactional);
        }
    }

    public static IEnumerable<TestFixtureData> Lease
    {
        get
        {
            yield return new TestFixtureData("postgres:17", PostgreSqlMessagePumpMode.Lease);
            yield return new TestFixtureData("postgres:18", PostgreSqlMessagePumpMode.Lease);
        }
    }
}
