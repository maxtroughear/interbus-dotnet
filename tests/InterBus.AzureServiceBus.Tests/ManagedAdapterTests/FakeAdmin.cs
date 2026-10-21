using Azure.Messaging.ServiceBus.Administration;

namespace InterBus.AzureServiceBus.Tests;

public partial class ManagedAdapterTests
{
    private sealed class FakeAdmin : ServiceBusAdministrationClient;
}
