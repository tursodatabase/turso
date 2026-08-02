# Turso.Platform.Client

.NET client for the [Turso Platform API](https://docs.turso.tech/api-reference/introduction) (`https://api.turso.tech`): manage databases, groups, auth tokens, organizations, members and invites.

Unlike the `Turso` and `Turso.Data.Sqlite` packages, this is a pure managed HTTP client — it has no native component.

## Getting started

```C#
using Turso.Platform.Client;

var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Authorization =
    new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", platformApiToken);

var client = new TursoPlatformClient(httpClient);

// Create a database
var created = await client.CreateDatabaseAsync(organizationSlug, new CreateDatabaseInput
{
    Name = "my-database",
    Group = "default",
});

// Mint a database auth token
var token = await client.CreateDatabaseTokenAsync(organizationSlug, "my-database");

// List databases
var databases = await client.ListDatabasesAsync(organizationSlug);
```

The client is generated with [NSwag](https://github.com/RicoSuter/NSwag) from the official OpenAPI specification published in [tursodatabase/turso-docs](https://github.com/tursodatabase/turso-docs) (`api-reference/openapi.json`).

## Regenerating the client

The pinned upstream spec lives at `openapi.json` next to this file and is kept byte-identical to
the published one. Because the spec leaves its request/response schemas anonymous, NSwag would
generate counter names (`Response17`, `Body3`); the `tools/OpenApiTitler` project first derives
proper names from each `operationId` (`ListDatabasesResponse`, `CreateGroupRequest`) and collapses
the repeated `{ error: string }` payloads into one shared `ErrorResponse`.

Every `dotnet build`, test, or pack generates the transformed document and C# client under the
project's ignored `obj` directory. The build uses the version-pinned `NSwag.MSBuild` package, so it
does not require a globally installed NSwag CLI.

To refresh the tracked spec and regenerate during the next build:

```bash
curl -sL https://raw.githubusercontent.com/tursodatabase/turso-docs/main/api-reference/openapi.json -o openapi.json
dotnet build
```

(or `make generate-platform-client` from `bindings/dotnet`).

`TursoPlatformClient.Supplement.cs` contains the hand-written `Extensions` union NSwag cannot generate. Use `Extensions.All` to enable every extension or `Extensions.FromNames(...)` for an explicit list.

Spec pinned from turso-docs commit `ac44bfab64a7` (2026-06-08).
