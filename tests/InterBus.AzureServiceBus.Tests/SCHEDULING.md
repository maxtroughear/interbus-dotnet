# Azure Service Bus test modes

Local and PR runs use the emulator. Queue cancellation cases are skipped with an NUnit
assumption because of [emulator issue #119](https://github.com/Azure/azure-service-bus-emulator-installer/issues/119).
Microsoft marked the bug fixed on 2026-10-06 for the next emulator image release. Our image
still reproduced it on 2026-10-23, including cancellation at least five seconds before activation.
The tested local image was built on 2026-01-12, with digest
`sha256:a00c9626c8960f6b9be6178aa91a7ac8f1a102c0d9deda7603a1ba0ac9d9ab51`.
The `latest` tag may be cached locally. Remove the skip after verifying an image containing the fix. Topic cancellation and all delivery
cases continue to run locally. Provider unit tests always verify exact cancellation arguments.

A retest after pulling `latest` on 2026-10-23 used the image built on 2026-07-01,
with digest `sha256:5a96d893b245031740f7d46e0fe5ff282d24b78c4b7d761dd57590f3f010a9b3`.
With the skip removed, native scheduling tests reported 7 passed and 1 failed: queue
cancellation still allowed delivery after cancellation completed well before activation.
Topic cancellation passed. The emulator queue cancellation skip remains in place.

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

The Azure path has been built but needs its first authenticated run after infrastructure setup.

References:
- [Federated credential setup](https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation-create-trust-user-assigned-managed-identity)
- [Azure Login OIDC](https://github.com/Azure/login#login-with-openid-connect-oidc-recommended)
- [Service Bus RBAC roles](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/integration#azure-service-bus-data-owner)
