using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using AwesomeAssertions;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso.Tests;

public sealed class TursoSyncInteropTests
{
    [Test]
    public void NativeLayoutsMatchTheSyncHeader()
    {
        var assembly = typeof(TursoSyncBindings).Assembly;

        AssertLayout(
            GetNativeType(assembly, "TursoSliceRef"),
            [
                ("Pointer", Pointer()),
                ("Length", Pointer()),
            ]);
        AssertLayout(
            GetNativeType(assembly, "TursoSyncDatabaseConfigNative"),
            [
                ("Path", Pointer()),
                ("RemoteUrl", Pointer()),
                ("ClientName", Pointer()),
                ("LongPollTimeoutMs", Int32()),
                ("BootstrapIfEmpty", Bool()),
                ("ReservedBytes", Int32()),
                ("PartialBootstrapStrategyPrefix", Int32()),
                ("PartialBootstrapStrategyQuery", Pointer()),
                ("PartialBootstrapSegmentSize", Pointer()),
                ("PartialBootstrapPrefetch", Bool()),
                ("RemoteEncryptionKey", Pointer()),
                ("RemoteEncryptionCipher", Pointer()),
                ("PushOperationsThreshold", Pointer()),
                ("PullBytesThreshold", Pointer()),
                ("LogicalMvccPull", Bool()),
            ]);
        AssertLayout(
            GetNativeType(assembly, "TursoSyncHttpRequestNative"),
            [
                ("Url", Slice()),
                ("Method", Slice()),
                ("Path", Slice()),
                ("Body", Slice()),
                ("HeaderCount", Int32()),
            ]);
        AssertLayout(
            GetNativeType(assembly, "TursoSyncHttpHeaderNative"),
            [
                ("Key", Slice()),
                ("Value", Slice()),
            ]);
        AssertLayout(
            GetNativeType(assembly, "TursoSyncFullReadRequestNative"),
            [("Path", Slice())]);
        AssertLayout(
            GetNativeType(assembly, "TursoSyncFullWriteRequestNative"),
            [
                ("Path", Slice()),
                ("Content", Slice()),
            ]);
        AssertLayout(
            GetNativeType(assembly, "TursoSyncStatsNative"),
            [
                ("CdcOperations", Int64()),
                ("MainWalSize", Int64()),
                ("RevertWalSize", Int64()),
                ("LastPullUnixTime", Int64()),
                ("LastPushUnixTime", Int64()),
                ("NetworkSentBytes", Int64()),
                ("NetworkReceivedBytes", Int64()),
                ("Revision", Slice()),
            ]);

        foreach (var fieldName in new[]
                 {
                     "BootstrapIfEmpty",
                     "PartialBootstrapPrefetch",
                     "LogicalMvccPull",
                 })
        {
            var field = GetNativeType(assembly, "TursoSyncDatabaseConfigNative").GetField(fieldName)!;
            field.GetCustomAttribute<MarshalAsAttribute>()!.Value.Should().Be(UnmanagedType.I1);
        }

        Enum.GetUnderlyingType(GetNativeType(assembly, "TursoSyncIoRequestType")).Should().Be(typeof(int));
        Enum.GetUnderlyingType(GetNativeType(assembly, "TursoSyncOperationResultType")).Should().Be(typeof(int));
    }

    [Test]
    public void NewDatabaseAcceptsAnOmittedRemoteUrl()
    {
        using var database = TursoSyncBindings.NewDatabase(
            new TursoSyncDatabaseConfiguration
            {
                Path = ":memory:",
            });

        database.IsInvalid.Should().BeFalse();
    }

    [Test]
    public void SyncCreateProducesAReadableHttpIoRequest()
    {
        using var database = TursoSyncBindings.NewDatabase(
            new TursoSyncDatabaseConfiguration
            {
                Path = ":memory:",
                RemoteUrl = "https://example.test",
                ClientName = "turso-dotnet-interop-test",
            });
        using var operation = TursoSyncBindings.StartCreate(database);
        using var item = TakeNextIoItem(database, operation);

        TursoSyncBindings.GetIoKind(item).Should().Be(TursoSyncIoKind.Http);
        var request = TursoSyncBindings.GetHttpRequest(item);
        request.Method.Should().Be("POST");
        request.Path.Should().Be("/pull-updates");
        request.Headers.Should().Contain(x => x.Name == "content-type" && x.Value == "application/protobuf");

        TursoSyncBindings.PoisonIo(item, "test stopped before network I/O");
        TursoSyncBindings.CompleteIo(item);
        TursoSyncBindings.StepIoCallbacks(database);
    }

    [Test]
    public void HttpIoAcceptsStatusAndResponseBytes()
    {
        using var database = TursoSyncBindings.NewDatabase(
            new TursoSyncDatabaseConfiguration
            {
                Path = ":memory:",
                RemoteUrl = "https://example.test",
                ClientName = "turso-dotnet-interop-test",
            });
        using var operation = TursoSyncBindings.StartCreate(database);
        using var item = TakeNextIoItem(database, operation);
        var body = Encoding.UTF8.GetBytes("not a sync response");

        TursoSyncBindings.SetIoStatus(item, 500);
        TursoSyncBindings.PushIoBuffer(item, body);
        TursoSyncBindings.CompleteIo(item);
        TursoSyncBindings.StepIoCallbacks(database);
    }

    [Test]
    public void ExtractedConnectionKeepsItsSyncDatabaseAlive()
    {
        var database = TursoSyncBindings.NewDatabase(
            new TursoSyncDatabaseConfiguration
            {
                Path = ":memory:",
                RemoteUrl = "https://example.test",
                ClientName = "turso-dotnet-ownership-test",
                BootstrapIfEmpty = false,
            });
        using var create = TursoSyncBindings.StartCreate(database);
        DriveLocalOperation(database, create);

        using var connect = TursoSyncBindings.StartConnect(database);
        DriveLocalOperation(database, connect);
        TursoSyncBindings.GetResultKind(connect).Should().Be(TursoSyncOperationResultKind.Connection);
        var connection = TursoSyncBindings.ExtractConnection(database, connect);

        database.Dispose();
        database.IsClosed.Should().BeFalse();

        using (connection)
        {
            using var statement = TursoBindings.PrepareStatement(connection, "SELECT 1");
            TursoBindings.Read(statement).Should().BeTrue();
            TursoBindings.GetValue(statement, 0).IntValue.Should().Be(1L);
            TursoBindings.Read(statement).Should().BeFalse();
        }

        database.IsClosed.Should().BeTrue();
    }

    [Test]
    public void NativePackageKeepsLegacyRidFilenames()
    {
        var makefile = File.ReadAllText(FindRepositoryFile("bindings", "dotnet", "Makefile"));
        makefile.Should().Contain("cargo build -p turso_sync_sdk_kit");
        makefile.Should().Contain("turso_sync_sdk_kit.dll' './rs_compiled/x86_64-pc-windows-msvc/$(RUST_PROFILE_DIR)/turso_sdk_kit.dll");
        makefile.Should().Contain("libturso_sync_sdk_kit.so ./rs_compiled/x86_64-unknown-linux-gnu/$(RUST_PROFILE_DIR)/libturso_sdk_kit.so");
        makefile.Should().Contain("libturso_sync_sdk_kit.dylib ./rs_compiled/x86_64-apple-darwin/$(RUST_PROFILE_DIR)/libturso_sdk_kit.dylib");

        var project = File.ReadAllText(
            FindRepositoryFile("bindings", "dotnet", "src", "Turso.Raw", "Turso.Raw.csproj"))
            .Replace('\\', '/');
        project.Should().NotContain("turso_sync_sdk_kit");

        foreach (var packagePath in new[]
                 {
                     "runtimes/win-x64/native/turso_sdk_kit.dll",
                     "runtimes/win-arm64/native/turso_sdk_kit.dll",
                     "runtimes/linux-x64/native/libturso_sdk_kit.so",
                     "runtimes/linux-arm64/native/libturso_sdk_kit.so",
                     "runtimes/osx-x64/native/libturso_sdk_kit.dylib",
                     "runtimes/osx-arm64/native/libturso_sdk_kit.dylib",
                     "runtimes/android-arm64/native/libturso_sdk_kit.so",
                     "runtimes/android-arm/native/libturso_sdk_kit.so",
                     "runtimes/android-x64/native/libturso_sdk_kit.so",
                     "runtimes/android-x86/native/libturso_sdk_kit.so",
                     "runtimes/ios-universal/native/libturso_sdk_kit.xcframework",
                 })
        {
            project.Should().Contain(packagePath);
        }
    }

    private static Type GetNativeType(Assembly assembly, string name)
        => assembly.GetType($"Turso.Raw.{name}", throwOnError: true)!;

    private static NativeField Pointer() => new(IntPtr.Size, IntPtr.Size);

    private static NativeField Int32() => new(sizeof(int), sizeof(int));

    private static NativeField Int64() => new(sizeof(long), Math.Min(sizeof(long), IntPtr.Size));

    private static NativeField Bool() => new(sizeof(byte), sizeof(byte));

    private static NativeField Slice() => new(IntPtr.Size * 2, IntPtr.Size);

    private static void AssertLayout(Type type, IReadOnlyList<(string Name, NativeField Field)> fields)
    {
        var offset = 0;
        var structAlignment = 1;
        foreach (var (name, field) in fields)
        {
            offset = Align(offset, field.Alignment);
            Marshal.OffsetOf(type, name).ToInt32().Should().Be(offset, $"{type.Name}.{name} must match the C header");
            offset += field.Size;
            structAlignment = Math.Max(structAlignment, field.Alignment);
        }

        Marshal.SizeOf(type).Should().Be(Align(offset, structAlignment), $"{type.Name} must match the C header");
    }

    private static int Align(int value, int alignment)
        => checked((value + alignment - 1) / alignment * alignment);

    private static TursoSyncIoItemHandle TakeNextIoItem(
        TursoSyncDatabaseHandle database,
        TursoSyncOperationHandle operation)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var state = TursoSyncBindings.Resume(operation);
            state.Should().NotBe(TursoSyncOperationState.Done);
            if (state != TursoSyncOperationState.Io)
                continue;

            var item = TursoSyncBindings.TakeIoItem(database);
            if (item is not null)
                return item;
        }

        throw new AssertionException("The sync operation did not produce an I/O item.");
    }

    private static void DriveLocalOperation(
        TursoSyncDatabaseHandle database,
        TursoSyncOperationHandle operation)
    {
        for (var attempt = 0; attempt < 1_000; attempt++)
        {
            switch (TursoSyncBindings.Resume(operation))
            {
                case TursoSyncOperationState.Continue:
                    continue;
                case TursoSyncOperationState.Done:
                    return;
                case TursoSyncOperationState.Io:
                    using (var item = TursoSyncBindings.TakeIoItem(database))
                    {
                        item.Should().NotBeNull();
                        switch (TursoSyncBindings.GetIoKind(item!))
                        {
                            case TursoSyncIoKind.FullRead:
                                TursoSyncBindings.PushIoBuffer(item!, []);
                                break;
                            case TursoSyncIoKind.FullWrite:
                                _ = TursoSyncBindings.GetFullWriteRequest(item!);
                                break;
                            default:
                                throw new AssertionException("A deferred local create must not require HTTP I/O.");
                        }

                        TursoSyncBindings.CompleteIo(item!);
                    }
                    TursoSyncBindings.StepIoCallbacks(database);
                    break;
                default:
                    throw new AssertionException("Unknown sync operation state.");
            }
        }

        throw new AssertionException("The sync operation did not complete.");
    }

    private static string FindRepositoryFile(params string[] path)
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory);
             directory is not null;
             directory = directory.Parent)
        {
            var candidate = Path.Combine([directory.FullName, .. path]);
            if (File.Exists(candidate))
                return candidate;
        }

        throw new FileNotFoundException($"Could not find repository file {Path.Combine(path)}.");
    }

    private readonly record struct NativeField(int Size, int Alignment);
}
