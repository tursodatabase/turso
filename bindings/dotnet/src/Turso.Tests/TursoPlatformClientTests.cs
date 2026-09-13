using System.Text.Json;
using AwesomeAssertions;
using Turso.Platform.Client;

namespace Turso.Tests;

public class TursoPlatformClientTests
{
    [Test]
    public void GroupExtensionsSerializeAndDeserializeBothSupportedForms()
    {
        var named = new NewGroup
        {
            Name = "test",
            Location = "aws-us-east-1",
            Extensions = Extensions.FromNames(["fts5", "vector"]),
        };

        var namedJson = JsonSerializer.Serialize(named);
        using var namedDocument = JsonDocument.Parse(namedJson);
        namedDocument.RootElement.GetProperty("extensions").EnumerateArray()
            .Select(static extension => extension.GetString())
            .Should().Equal("fts5", "vector");

        var allJson = JsonSerializer.Serialize(new NewGroup
        {
            Name = "test",
            Location = "aws-us-east-1",
            Extensions = Extensions.All,
        });
        using var allDocument = JsonDocument.Parse(allJson);
        allDocument.RootElement.GetProperty("extensions").GetString().Should().Be("all");

        var all = JsonSerializer.Deserialize<NewGroup>(
            """{"name":"test","location":"aws-us-east-1","extensions":"all"}""");
        all!.Extensions.Should().BeSameAs(Extensions.All);

        var deserializedNames = JsonSerializer.Deserialize<NewGroup>(
            """{"name":"test","location":"aws-us-east-1","extensions":["fts5","vector"]}""");
        deserializedNames!.Extensions!.Names.Should().Equal("fts5", "vector");
    }
}
