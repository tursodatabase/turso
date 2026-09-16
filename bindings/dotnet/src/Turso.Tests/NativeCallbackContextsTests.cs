using AwesomeAssertions;
using Turso.Data.Sqlite;

namespace Turso.Tests;

public class NativeCallbackContextsTests
{
    [Test]
    public void AContextIsNeverHandedOutAgainAfterItIsRemoved()
    {
        var first = NativeCallbackContexts.Add(new Registration("scalar"));
        NativeCallbackContexts.Remove(first).Should().NotBeNull();

        var replacement = new Registration("aggregate");
        var second = NativeCallbackContexts.Add(replacement);

        second.Should().NotBe(first);
        NativeCallbackContexts.Find<Registration>(first).Should().BeNull();
        NativeCallbackContexts.Find<Registration>(second).Should().BeSameAs(replacement);

        NativeCallbackContexts.Remove(second);
    }

    [Test]
    public void AnUnknownContextFindsNothingAndRemovesNothing()
    {
        var live = new Registration("scalar");
        var liveContext = NativeCallbackContexts.Add(live);

        NativeCallbackContexts.Find<Registration>(IntPtr.Zero).Should().BeNull();
        NativeCallbackContexts.Remove(IntPtr.Zero).Should().BeNull();
        NativeCallbackContexts.Remove(IntPtr.Add(liveContext, 1_000_000)).Should().BeNull();
        NativeCallbackContexts.Find<Registration>(liveContext).Should().BeSameAs(live);

        NativeCallbackContexts.Remove(liveContext);
    }

    [Test]
    public void AContextIsNeverGivenBackAsAnotherKind()
    {
        var context = NativeCallbackContexts.Add(new Registration("scalar"));

        NativeCallbackContexts.Find<string>(context).Should().BeNull();
        NativeCallbackContexts.Find<Registration>(context).Should().NotBeNull();

        NativeCallbackContexts.Remove(context);
    }

    private sealed class Registration(string kind)
    {
        public string Kind { get; } = kind;
    }
}
