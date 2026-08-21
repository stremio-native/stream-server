//! Build script for async-rar
//!
//! Compiles the UnRAR C++ library and generates autocxx bindings.
//! Supports Windows, Linux, and macOS.
//!
//! UnRAR has complex include dependencies between cpp files, so we use
//! a "unity build" approach with a single source file that includes
//! everything in the correct order.

use std::env;
use std::path::PathBuf;

mod build_support;
use build_support::should_compile_unrar_cpp;

fn find_llvm_path() -> Option<PathBuf> {
    if env::var("LIBCLANG_PATH").is_ok() {
        return None;
    }

    let program_files =
        env::var("ProgramFiles").unwrap_or_else(|_| r"C:\Program Files".to_string());
    let program_files_x86 =
        env::var("ProgramFiles(x86)").unwrap_or_else(|_| r"C:\Program Files (x86)".to_string());

    let search_paths = vec![
        format!(r"{}\LLVM\bin", program_files),
        format!(r"{}\LLVM\bin", program_files_x86),
        format!(
            r"{}\Microsoft Visual Studio\2022\Community\VC\Tools\Llvm\x64\bin",
            program_files
        ),
        format!(
            r"{}\Microsoft Visual Studio\2022\Enterprise\VC\Tools\Llvm\x64\bin",
            program_files
        ),
        format!(
            r"{}\Microsoft Visual Studio\2022\Professional\VC\Tools\Llvm\x64\bin",
            program_files
        ),
        r"C:\ProgramData\chocolatey\bin".to_string(),
        r"C:\ProgramData\chocolatey\lib\llvm\tools\llvm\bin".to_string(),
    ];

    for path in search_paths {
        let p = PathBuf::from(path);
        if p.join("libclang.dll").exists() {
            return Some(p);
        }
    }

    // Check scoop
    if let Ok(profile) = env::var("USERPROFILE") {
        let p = PathBuf::from(profile).join(r"scoop\apps\llvm\current\bin");
        if p.join("libclang.dll").exists() {
            return Some(p);
        }
    }

    None
}

fn find_include_paths() -> Vec<PathBuf> {
    let mut includes = Vec::new();
    let program_files =
        env::var("ProgramFiles").unwrap_or_else(|_| r"C:\Program Files".to_string());
    let program_files_x86 =
        env::var("ProgramFiles(x86)").unwrap_or_else(|_| r"C:\Program Files (x86)".to_string());

    // 1. Find MSVC Includes
    let vs_editions = ["Enterprise", "Professional", "Community", "BuildTools"];
    let vs_years = ["2022", "2019", "2017"];
    let mut msvc_path = None;

    for year in vs_years {
        for edition in vs_editions {
            // Check both Program Files directories
            for root in &[&program_files, &program_files_x86] {
                let p = PathBuf::from(format!(
                    r"{}\Microsoft Visual Studio\{}\{}\VC\Tools\MSVC",
                    root, year, edition
                ));
                if p.exists() {
                    // Find latest version
                    if let Ok(entries) = std::fs::read_dir(&p)
                        && let Some(latest) = entries
                            .filter_map(Result::ok)
                            .filter(|e| e.path().is_dir())
                            .map(|e| e.path())
                            .max()
                    {
                        let include = latest.join("include");
                        if include.exists() {
                            msvc_path = Some(include);
                            break;
                        }
                    }
                }
            }
            if msvc_path.is_some() {
                break;
            }
        }
        if msvc_path.is_some() {
            break;
        }
    }

    if let Some(p) = msvc_path {
        includes.push(p);
    }

    // 2. Find Windows SDK Includes
    let sdk_roots = vec![
        PathBuf::from(format!(r"{}\Windows Kits\10\Include", program_files_x86)),
        PathBuf::from(format!(r"{}\Windows Kits\10\Include", program_files)),
    ];

    let mut found_sdk = false;
    for sdk_root in sdk_roots {
        if let Ok(entries) = std::fs::read_dir(&sdk_root) {
            // Find latest version that actually contains required headers.
            let mut candidates: Vec<PathBuf> = entries
                .filter_map(Result::ok)
                .filter(|e| e.path().is_dir())
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name().unwrap().to_str().unwrap().starts_with("10.")
                        && p.join("um").join("Windows.h").exists()
                        && p.join("ucrt").exists()
                })
                .collect();
            candidates.sort();
            if let Some(latest) = candidates.pop() {
                includes.push(latest.join("ucrt"));
                includes.push(latest.join("shared"));
                includes.push(latest.join("um"));
                found_sdk = true;
                break;
            }
        }
    }

    if !found_sdk {
        // println!("cargo:warning=Could not find Windows SDK includes");
    }

    includes
}

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let is_windows = target_os == "windows";
    let is_macos = target_os == "macos";
    let is_linux = target_os == "linux";
    let is_android = target_os == "android";

    let mut extra_includes = Vec::new();

    // Attempt to locate libclang on Windows if not set
    if is_windows {
        if let Some(path) = find_llvm_path() {
            println!("cargo:rustc-env=LIBCLANG_PATH={}", path.display());
            unsafe {
                env::set_var("LIBCLANG_PATH", path);
            }
        }

        // Find and set include paths for autocxx/bindgen
        if env::var("INCLUDE").is_err() {
            let includes = find_include_paths();
            let mut args = String::new();
            for inc in includes {
                let p = inc.to_string_lossy();
                extra_includes.push(format!("-I{}", p));
                args.push_str(&format!("-I\"{}\" ", p));
            }
            if !args.is_empty() {
                println!("cargo:rustc-env=BINDGEN_EXTRA_CLANG_ARGS={}", args);
                // Also set env var for current process (autocxx build)
                unsafe {
                    env::set_var("BINDGEN_EXTRA_CLANG_ARGS", args);
                }
            }
        }
    }

    let unrar_src = PathBuf::from("vendor/unrar");

    if !unrar_src.exists() {
        panic!(
            "UnRAR source not found at {:?}. Please download from https://www.rarlab.com/rar_add.htm",
            unrar_src
        );
    }

    // Collect all C++ source files
    let mut cpp_files: Vec<PathBuf> = glob::glob("vendor/unrar/*.cpp")
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .filter(|p| {
            let name = p.file_name().unwrap().to_str().unwrap();
            should_compile_unrar_cpp(name, is_windows)
        })
        .collect();

    // Add stubs for missing symbols
    cpp_files.push(PathBuf::from("src/cpp/stubs.cpp"));

    // Build UnRAR as a C++ static library
    let mut cc_build = cc::Build::new();
    cc_build
        .cpp(true)
        .include(&unrar_src)
        .files(&cpp_files)
        .define("RARDLL", None) // Build as DLL/library mode
        .define("SILENT", None); // Disable console output

    // Platform-specific settings
    if is_windows {
        cc_build
            .define("_WIN_ALL", None)
            .flag("/std:c++17")
            .flag("/EHsc") // Enable C++ exceptions
            // Force include rar.hpp - UnRAR uses precompiled headers
            .flag("/FIrar.hpp");
    } else {
        cc_build
            .flag("-std=c++17")
            .flag("-fPIC")
            .flag("-pthread")
            // Force include rar.hpp - UnRAR uses precompiled headers
            .flag("-include")
            .flag("rar.hpp")
            .flag("-Wno-unused-parameter")
            .flag("-Wno-parentheses")
            .flag("-Wno-dangling-else")
            .flag("-Wno-sign-compare")
            .flag("-Wno-unused-variable")
            .flag("-Wno-unused-function")
            .flag("-Wno-switch")
            .flag("-Wno-unused-but-set-variable")
            .flag("-Wno-misleading-indentation")
            .flag_if_supported("-Wno-catch-value")
            .flag_if_supported("-Wno-class-memaccess")
            .flag("-Wno-comment")
            .flag("-Wno-extra");

        if is_macos {
            cc_build.define("_APPLE", None);
        }

        if is_linux {
            cc_build.define("_UNIX", None);
        }
        // Note: for Android, _UNIX is already defined by raros.hpp when __ANDROID__ is set
    }

    cc_build.compile("unrar");

    // Generate autocxx bindings
    let mut clang_args = vec!["-DRARDLL".to_string(), "-DSILENT".to_string()];

    if is_windows {
        clang_args.extend(vec!["-D_WIN_ALL".to_string(), "-std=c++17".to_string()]);
    }
    if is_macos {
        clang_args.extend(vec!["-D_APPLE".to_string(), "-std=c++17".to_string()]);
    }
    if is_linux {
        clang_args.extend(vec!["-D_UNIX".to_string(), "-std=c++17".to_string()]);
    }
    if is_android {
        // Don't define _UNIX for Android - raros.hpp already defines it when __ANDROID__ is set
        clang_args.push("-std=c++17".to_string());

        if let Ok(target) = env::var("TARGET") {
            clang_args.push(format!("--target={}", target));
        }
        if let Ok(ndk) = env::var("ANDROID_NDK_HOME").or_else(|_| env::var("ANDROID_NDK_ROOT")) {
            let host_tag = if cfg!(target_os = "windows") {
                "windows-x86_64"
            } else if cfg!(target_os = "macos") {
                "darwin-x86_64"
            } else {
                "linux-x86_64"
            };
            let sysroot = format!("{}/toolchains/llvm/prebuilt/{}/sysroot", ndk, host_tag);
            clang_args.push(format!("--sysroot={}", sysroot));

            // Find clang's builtin header dir in NDK
            let clang_dir = PathBuf::from(&ndk)
                .join("toolchains")
                .join("llvm")
                .join("prebuilt")
                .join(host_tag)
                .join("lib")
                .join("clang");
            if let Ok(entries) = std::fs::read_dir(&clang_dir) {
                for entry in entries.filter_map(Result::ok) {
                    let include_path = entry.path().join("include");
                    if include_path.join("stddef.h").exists() {
                        clang_args.push(format!("-isystem{}", include_path.display()));
                        break;
                    }
                }
            }
        }
    }

    // Add explicitly found includes
    for inc in extra_includes {
        clang_args.push(inc);
    }

    let clang_args_refs: Vec<&str> = clang_args.iter().map(|s| s.as_str()).collect();

    let mut b = autocxx_build::Builder::new(
        "src/ffi.rs",
        [unrar_src.as_path(), PathBuf::from("src/cpp").as_path()],
    )
    .extra_clang_args(&clang_args_refs)
    .build()
    .expect("Failed to generate autocxx bindings");

    if is_windows {
        b.flag("/std:c++17");
    } else {
        b.flag("-std=c++17");
    }

    b.compile("async-rar-autocxx");

    // Rerun triggers
    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed=vendor/unrar");
}
