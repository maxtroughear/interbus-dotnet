using Azure.Messaging.ServiceBus.Administration;

namespace InterBus.AzureServiceBus.Tests;

public partial class SessionAdapterTests
{
    private sealed class Admin : ServiceBusAdministrationClient;
}
