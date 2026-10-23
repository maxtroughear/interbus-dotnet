# Azure Service Bus test modes

Local and PR runs use the emulator. Queue cancellation cases are skipped with an NUnit
assumption because of [emulator issue #119](https://github.com/Azure/azure-service-bus-emulator-installer/issues/119).
Remove the skip after verifying an emulator image containing the fix. Topic cancellation and
all delivery cases continue to run locally. Provider unit tests always verify exact cancellation arguments.

## Azure and Entra configuration

1. Create a dedicated Service Bus namespace using Standard or Premium (the tests need topics
   and sessions). Allow the GitHub runner network access to the namespace.
2. Create a user-assigned managed identity, or an Entra app registration with a service principal.
3. Add a GitHub federated credential to that identity, using these exact values:
   - Issuer: `https://token.actions.githubusercontent.com`
   - Subject: `repo:maxtroughear/interbus-dotnet:ref:refs/heads/main`
   - Audience: `api://AzureADTokenExchange`
   - Portal entity type: Branch; branch: `main`
4. Assign **Azure Service Bus Data Owner** to the identity's principal at the test namespace
   scope. Tests create/delete entities as well as send/receive messages. No Microsoft Graph
   application permissions or client secret is needed. Allow time for RBAC propagation.

## GitHub configuration

Add these repository Actions secrets under Settings > Secrets and variables > Actions > Secrets:

| Variable | Value |
| --- | --- |
| `AZURE_CLIENT_ID` | Managed identity client ID or app registration Application (client) ID |
| `AZURE_TENANT_ID` | Directory (tenant) ID |
| `AZURE_SUBSCRIPTION_ID` | Subscription containing the test namespace |
| `AZURE_SERVICEBUS_NAMESPACE` | Host name, such as `interbus-tests.servicebus.windows.net` (no scheme) |

The `test-azure-service-bus-azure` job runs only on a push to `main`, grants `id-token: write`
and `contents: read`, and uses `azure/login@v3`. The fixture uses `AzureCliCredential` to consume
that login, with `INTERBUS_USE_REAL_AZURE=true` and `INTERBUS_AZURE_SERVICEBUS_NAMESPACE` set.
All cancellation tests run in that mode. Each test gets unique topic/queue prefixes and teardown
deletes only entities matching its prefix. An interrupted runner can leave entities to clean up.
No Azure job environment is configured: adding one changes the OIDC subject to an environment
subject, requiring a corresponding federated credential and main-only environment branch policy.

To run against Azure locally, use `az login`, set those two `INTERBUS_*` variables, and run:

```sh
dotnet test --project tests/InterBus.AzureServiceBus.Tests/InterBus.AzureServiceBus.Tests.csproj
```

References:
- [Federated credential setup](https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation-create-trust-user-assigned-managed-identity)
- [Azure Login OIDC](https://github.com/Azure/login#login-with-openid-connect-oidc-recommended)
- [Service Bus RBAC roles](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/integration#azure-service-bus-data-owner)
