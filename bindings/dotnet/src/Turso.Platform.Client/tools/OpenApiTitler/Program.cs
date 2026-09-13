using System.Text.Json;
using System.Text.Json.Nodes;

// Names the anonymous inline schemas of an OpenAPI document so NSwag does not fall back to
// counter names (`Response17`, `Body3`):
//  - 2xx response and request-body schemas get a `title` derived from the `operationId`
//    (`listDatabases` -> `ListDatabasesResponse` / `CreateGroupRequest`); NSwag uses the title
//    as the generated class name.
//  - error (non-2xx) response schemas are lifted into shared `components/schemas` entries and
//    replaced with `$ref`s, deduplicated by structural shape — the Turso API reuses one
//    `{ error: string }` shape, so they all collapse into a single `ErrorResponse` class.
// The upstream document is left untouched; the transformed copy is consumed by nswag.json.
//
// Usage: OpenApiTitler <input-openapi.json> <output-openapi.json>

if (args.Length != 2)
{
    Console.Error.WriteLine("Usage: OpenApiTitler <input-openapi.json> <output-openapi.json>");
    return 1;
}

var root = JsonNode.Parse(File.ReadAllText(args[0]))
    ?? throw new InvalidOperationException("Failed to parse OpenAPI document.");

string[] methods = ["get", "put", "post", "delete", "options", "head", "patch", "trace"];
var titled = 0;
var lifted = 0;

var componentSchemas = root["components"]?["schemas"] as JsonObject
    ?? throw new InvalidOperationException("Document has no components/schemas section.");
var errorShapes = new Dictionary<string, string>(); // structural signature -> component name
NormalizeExtensionsUnion(componentSchemas);

if (root["paths"] is JsonObject paths)
{
    foreach (var (_, pathValue) in paths)
    {
        if (pathValue is not JsonObject pathItem)
        {
            continue;
        }

        foreach (var method in methods)
        {
            if (pathItem[method] is not JsonObject operation)
            {
                continue;
            }

            var operationName = ToPascalCase(operation["operationId"]?.GetValue<string>());
            if (operationName is null)
            {
                continue;
            }

            TitleSchema(GetJsonSchema(operation["requestBody"]), $"{operationName}Request");

            if (operation["responses"] is not JsonObject responses)
            {
                continue;
            }

            // Collect the response codes that carry an inline schema so the success/error
            // suffixes only get a status-code disambiguator when one is actually needed.
            var successCodes = new List<string>();
            var errorCodes = new List<string>();
            foreach (var (code, response) in responses)
            {
                if (GetJsonSchema(response) is not null)
                {
                    (code.StartsWith('2') ? successCodes : errorCodes).Add(code);
                }
            }

            foreach (var code in successCodes)
            {
                var suffix = successCodes.Count > 1 ? code : "";
                TitleSchema(GetJsonSchema(responses[code]), $"{operationName}Response{suffix}");
            }

            foreach (var code in errorCodes)
            {
                var suffix = errorCodes.Count > 1 ? code : "";
                LiftErrorSchema(responses[code], $"{operationName}Error{suffix}");
            }
        }
    }
}

// Reusable error responses (components/responses) carry the same inline shapes; lift them too.
if (root["components"]?["responses"] is JsonObject componentResponses)
{
    foreach (var (name, response) in componentResponses)
    {
        LiftErrorSchema(response, name);
    }
}

File.WriteAllText(args[1], root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine($"Titled {titled} inline schemas, lifted {lifted} error schemas into {errorShapes.Count} shared component(s) -> {args[1]}");
return 0;

// Resolves requestBody/response -> content -> application/json -> schema.
JsonObject? GetJsonSchema(JsonNode? requestBodyOrResponse)
{
    return requestBodyOrResponse?["content"]?["application/json"]?["schema"] as JsonObject;
}

// Replaces an inline error schema with a $ref to a shared components/schemas entry, reusing the
// same component for structurally identical shapes. `fallbackName` names a shape seen first here.
void LiftErrorSchema(JsonNode? response, string fallbackName)
{
    var schema = GetJsonSchema(response);
    if (schema is null || schema.ContainsKey("$ref"))
    {
        return;
    }

    var signature = StructuralSignature(schema);
    if (!errorShapes.TryGetValue(signature, out var componentName))
    {
        // The ubiquitous { error: string } payload gets the canonical name; other shapes keep
        // their operation-derived fallback.
        componentName = errorShapes.Count == 0 && schema["properties"] is JsonObject props
                        && props.Count == 1 && props.ContainsKey("error")
            ? "ErrorResponse"
            : fallbackName;

        if (!componentSchemas.ContainsKey(componentName))
        {
            var component = schema.DeepClone().AsObject();
            component.Remove("title");
            componentSchemas[componentName] = component;
        }

        errorShapes[signature] = componentName;
    }

    var content = response!["content"]!["application/json"]!.AsObject();
    content["schema"] = new JsonObject { ["$ref"] = $"#/components/schemas/{componentName}" };
    lifted++;
}

// Canonical shape of a schema, ignoring documentation-only members.
static string StructuralSignature(JsonNode? node)
{
    if (node is JsonObject obj)
    {
        var parts = obj
            .Where(static p => p.Key is not ("description" or "example" or "title"))
            .OrderBy(static p => p.Key, StringComparer.Ordinal)
            .Select(static p => $"\"{p.Key}\":{StructuralSignature(p.Value)}");
        return "{" + string.Join(",", parts) + "}";
    }

    if (node is JsonArray arr)
    {
        return "[" + string.Join(",", arr.Select(StructuralSignature)) + "]";
    }

    return node?.ToJsonString() ?? "null";
}

void TitleSchema(JsonObject? schema, string title)
{
    // Only inline anonymous schemas: a $ref already has a component name, an existing title wins.
    if (schema is null || schema.ContainsKey("$ref") || schema.ContainsKey("title"))
    {
        return;
    }

    var type = schema["type"]?.GetValue<string>();
    if (type == "array")
    {
        // Top-level arrays stay collections; name their inline object items instead.
        if (schema["items"] is JsonObject items && !items.ContainsKey("$ref") && !items.ContainsKey("title")
            && items["type"]?.GetValue<string>() is null or "object")
        {
            items["title"] = $"{title}Item";
            titled++;
        }

        return;
    }

    if (type is null or "object")
    {
        schema["title"] = title;
        titled++;
    }
}

static void NormalizeExtensionsUnion(JsonObject componentSchemas)
{
    var extensions = componentSchemas["Extensions"] as JsonObject
        ?? throw new InvalidOperationException("Document has no Extensions schema.");
    if (extensions["oneOf"] is not JsonArray)
    {
        throw new InvalidOperationException("Extensions schema is no longer a oneOf union.");
    }

    // NSwag emits an enum for the first oneOf branch, losing the valid array form. An object
    // emits an extensible partial class so the supplement can model either JSON shape.
    extensions.Remove("oneOf");
    extensions["type"] = "object";
}

static string? ToPascalCase(string? operationId)
{
    if (string.IsNullOrWhiteSpace(operationId))
    {
        return null;
    }

    var parts = operationId.Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries);
    return string.Concat(parts.Select(static p => char.ToUpperInvariant(p[0]) + p[1..]));
}
