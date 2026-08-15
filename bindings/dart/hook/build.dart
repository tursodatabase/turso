// Build hook: produces the native Turso library and bundles it as a code
// asset, so consumers do not have to load a shared library themselves.
//
// By default this compiles the `turso_sqlite3` crate with cargo for whichever
// target Dart or Flutter asks for. To use a library you already built (a slice
// out of an XCFramework or AAR, say), point the hook at it from the
// pubspec of the app being built:
//
//     hooks:
//       user_defines:
//         turso:
//           library: /path/to/libturso_sqlite3.dylib

import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';

/// Asset name, matching `ffi-native.asset-id` in `ffigen.yaml`.
const _assetName = 'src/bindings.g.dart';

const _crate = 'turso_sqlite3';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;

    final code = input.config.code;
    final library = await _resolveLibrary(input, output);

    output.assets.code.add(
      CodeAsset(
        package: input.packageName,
        name: _assetName,
        linkMode: DynamicLoadingBundled(),
        file: library,
      ),
    );

    // Rebuild when the C surface or its crate manifest changes. This is not
    // every Rust source that feeds the library; cargo tracks those itself, and
    // listing them all here would cost more than it saves.
    final crateRoot = _repoRoot(input).resolve('bindings/c/');
    output.dependencies.addAll([
      crateRoot.resolve('src/lib.rs'),
      crateRoot.resolve('Cargo.toml'),
      crateRoot.resolve('include/sqlite3.h'),
    ]);

    stderr.writeln(
      'turso: bundled ${library.toFilePath()} for '
      '${code.targetOS} ${code.targetArchitecture}',
    );
  });
}

/// Returns the library to bundle, building it with cargo unless the app
/// supplied one through user-defines.
Future<Uri> _resolveLibrary(BuildInput input, BuildOutputBuilder output) async {
  final prebuilt = input.userDefines['library'] as String?;
  if (prebuilt != null) {
    final file = File(prebuilt);
    if (!file.existsSync()) {
      throw Exception(
        'turso: user-define `library` points at $prebuilt, which does not '
        'exist.',
      );
    }
    output.dependencies.add(file.uri);
    return file.absolute.uri;
  }
  return _cargoBuild(input);
}

/// Compiles the crate for the requested target and returns the built library.
Future<Uri> _cargoBuild(BuildInput input) async {
  final code = input.config.code;
  final os = code.targetOS;
  final target = _rustTarget(code);
  final repoRoot = _repoRoot(input);

  // Cargo insists on one output directory per invocation, and hooks run once
  // per architecture, so give each target its own to keep them from racing.
  final targetDir = input.outputDirectoryShared.resolve('cargo/');

  final useCargoNdk = os == OS.android;
  final executable = useCargoNdk ? 'cargo-ndk' : 'cargo';
  final arguments = <String>[
    if (useCargoNdk) ...[
      'ndk',
      '--target',
      target,
      '--platform',
      '${code.android.targetNdkApi}',
      'build',
    ] else ...[
      'build',
      '--target',
      target,
    ],
    '--package',
    _crate,
    '--target-dir',
    targetDir.toFilePath(),
  ];

  final result = await Process.run(
    executable,
    arguments,
    workingDirectory: repoRoot.toFilePath(),
    environment: {
      if (os == OS.iOS)
        'IPHONEOS_DEPLOYMENT_TARGET': '${code.iOS.targetVersion}',
      if (os == OS.macOS)
        'MACOSX_DEPLOYMENT_TARGET': '${code.macOS.targetVersion}',
    },
    runInShell: Platform.isWindows,
  );

  if (result.exitCode != 0) {
    throw Exception(
      'turso: `$executable ${arguments.join(' ')}` failed with exit code '
      '${result.exitCode}.\n${result.stderr}',
    );
  }

  final built = File.fromUri(
    targetDir.resolve('$target/debug/${_libraryFileName(os)}'),
  );
  if (!built.existsSync()) {
    throw Exception(
        'turso: cargo reported success but ${built.path} is missing.');
  }
  return built.absolute.uri;
}

/// The repository root, so cargo runs against the workspace rather than this
/// package.
Uri _repoRoot(BuildInput input) => input.packageRoot.resolve('../../');

String _libraryFileName(OS os) => switch (os) {
      OS.windows => '$_crate.dll',
      OS.macOS || OS.iOS => 'lib$_crate.dylib',
      _ => 'lib$_crate.so',
    };

/// Maps the Dart target onto a Rust target triple.
String _rustTarget(CodeConfig code) {
  final architecture = code.targetArchitecture;
  return switch (code.targetOS) {
    OS.macOS => switch (architecture) {
        Architecture.arm64 => 'aarch64-apple-darwin',
        Architecture.x64 => 'x86_64-apple-darwin',
        _ => throw UnsupportedError('turso: unsupported macOS $architecture'),
      },
    OS.iOS => switch ((architecture, code.iOS.targetSdk)) {
        (Architecture.arm64, IOSSdk.iPhoneOS) => 'aarch64-apple-ios',
        (Architecture.arm64, _) => 'aarch64-apple-ios-sim',
        (Architecture.x64, _) => 'x86_64-apple-ios',
        _ => throw UnsupportedError('turso: unsupported iOS $architecture'),
      },
    OS.android => switch (architecture) {
        Architecture.arm64 => 'aarch64-linux-android',
        Architecture.arm => 'armv7-linux-androideabi',
        Architecture.x64 => 'x86_64-linux-android',
        Architecture.ia32 => 'i686-linux-android',
        _ => throw UnsupportedError('turso: unsupported Android $architecture'),
      },
    OS.linux => switch (architecture) {
        Architecture.arm64 => 'aarch64-unknown-linux-gnu',
        Architecture.x64 => 'x86_64-unknown-linux-gnu',
        Architecture.riscv64 => 'riscv64gc-unknown-linux-gnu',
        _ => throw UnsupportedError('turso: unsupported Linux $architecture'),
      },
    OS.windows => switch (architecture) {
        Architecture.arm64 => 'aarch64-pc-windows-msvc',
        Architecture.x64 => 'x86_64-pc-windows-msvc',
        _ => throw UnsupportedError('turso: unsupported Windows $architecture'),
      },
    final os => throw UnsupportedError('turso: unsupported target OS $os'),
  };
}
