use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");

    if env::var("CARGO_CFG_WINDOWS").is_err() {
        return;
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set by Cargo"));
    let rc_path = out_dir.join("turso_sdk_kit_version.rc");
    let res_path = out_dir.join("turso_sdk_kit_version.res");
    let version = env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION is set by Cargo");
    let file_version = file_version(&version);
    let file_version_commas = file_version.replace('.', ",");
    let rc = format!(
        r#"#define VS_FFI_FILEFLAGSMASK 0x0000003FL
#define VOS_NT_WINDOWS32 0x00040004L
#define VFT_DLL 0x00000002L
#define VFT2_UNKNOWN 0x00000000L

1 VERSIONINFO
FILEVERSION {file_version_commas}
PRODUCTVERSION {file_version_commas}
FILEFLAGSMASK VS_FFI_FILEFLAGSMASK
FILEFLAGS 0
FILEOS VOS_NT_WINDOWS32
FILETYPE VFT_DLL
FILESUBTYPE VFT2_UNKNOWN
BEGIN
  BLOCK "StringFileInfo"
  BEGIN
    BLOCK "040904b0"
    BEGIN
      VALUE "CompanyName", "Turso\0"
      VALUE "FileDescription", "Turso SDK Kit\0"
      VALUE "FileVersion", "{file_version}\0"
      VALUE "InternalName", "turso_sdk_kit\0"
      VALUE "LegalCopyright", "Copyright (c) 2026 Turso\0"
      VALUE "OriginalFilename", "turso_sdk_kit.dll\0"
      VALUE "ProductName", "Turso\0"
      VALUE "ProductVersion", "{version}\0"
    END
  END
  BLOCK "VarFileInfo"
  BEGIN
    VALUE "Translation", 0x0409, 1200
  END
END
"#
    );
    fs::write(&rc_path, rc).expect("write Windows version resource script");

    let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    let status = if target_env == "msvc" {
        Command::new(find_windows_resource_compiler())
            .arg("/nologo")
            .arg(format!("/fo{}", res_path.display()))
            .arg(&rc_path)
            .status()
    } else {
        Command::new("windres")
            .arg(&rc_path)
            .arg("-O")
            .arg("coff")
            .arg("-o")
            .arg(&res_path)
            .status()
    }
    .expect("compile Windows version resource");

    assert!(
        status.success(),
        "Windows version resource compiler failed with status {status}"
    );

    println!("cargo::rustc-link-arg-cdylib={}", res_path.display());
}

fn find_windows_resource_compiler() -> PathBuf {
    if let Some(path) = find_in_path("rc.exe") {
        return path;
    }

    let target_arch =
        env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH is set by Cargo");
    let sdk = find_msvc_tools::find_windows_sdk(&target_arch)
        .expect("rc.exe was not found in PATH and no Windows SDK is registered");
    let sdk_bin_dirs: Vec<&Path> = sdk.path().collect();
    sdk_bin_dirs
        .iter()
        .map(|dir| dir.join("rc.exe"))
        .find(|path| path.exists())
        .unwrap_or_else(|| {
            panic!("rc.exe was not found in PATH or in Windows SDK directories {sdk_bin_dirs:?}")
        })
}

fn find_in_path(program: &str) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|path| path.join(program))
        .find(|candidate| candidate.exists())
}

fn file_version(version: &str) -> String {
    let core = version.split_once('-').map_or(version, |(core, _)| core);
    let mut parts = core.split('.').map(|part| {
        part.parse::<u16>()
            .expect("Cargo package version components fit Windows VERSIONINFO")
    });
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let patch = parts.next().unwrap_or(0);
    format!("{major}.{minor}.{patch}.0")
}
