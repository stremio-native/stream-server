use crate::{ServerConfig, ServerHandle};
use jni::Env;
use jni::EnvUnowned;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jstring;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Mutex;

static SERVER_HANDLE: Lazy<Mutex<Option<ServerHandle>>> = Lazy::new(|| Mutex::new(None));

#[cfg(target_os = "android")]
fn init_android_tls_verifier(env: &mut Env, context: JObject) -> jni::errors::Result<()> {
    rustls_platform_verifier::android::init_with_env(env, context)
}

#[cfg(not(target_os = "android"))]
fn init_android_tls_verifier(_env: &mut Env, _context: JObject) -> jni::errors::Result<()> {
    Ok(())
}

#[unsafe(no_mangle)]
/// Starts the embedded server from a JVM native call.
///
/// # Safety
///
/// The JNI environment and object handles must be valid for the duration of
/// this call and must originate from the invoking JVM thread.
pub unsafe extern "C" fn Java_com_stremio_mobile_server_JniStreamingServerController_startServerNative(
    mut env: EnvUnowned, _class: JClass, context: JObject, config_dir: JString, cache_dir: JString,
    port: jni::sys::jint,
) -> jstring {
    env.with_env(|env| -> jni::errors::Result<jstring> {
        init_android_tls_verifier(env, context)?;

        let config_dir_str: String = match config_dir.try_to_string(env) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new(
                    jni::jni_str!("java/lang/IllegalArgumentException"),
                    jni::jni_str!("Invalid configDir string"),
                );
                return Ok(std::ptr::null_mut());
            }
        };

        let cache_dir_str: String = match cache_dir.try_to_string(env) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new(
                    jni::jni_str!("java/lang/IllegalArgumentException"),
                    jni::jni_str!("Invalid cacheDir string"),
                );
                return Ok(std::ptr::null_mut());
            }
        };

        let mut handle_lock = SERVER_HANDLE.lock().unwrap();
        if handle_lock.is_some() {
            let bound_addr = handle_lock.as_ref().unwrap().bound_http_addr();
            let url = format!("http://{}", bound_addr);
            return Ok(env.new_string(url)?.into_raw());
        }

        let mut cfg = ServerConfig::embedded();
        cfg.config_dir = Some(PathBuf::from(config_dir_str));
        cfg.cache_dir = Some(PathBuf::from(cache_dir_str));
        cfg.http_addr = SocketAddr::from(([127, 0, 0, 1], port as u16));
        cfg.init_logging = true;

        match crate::start(cfg) {
            Ok(handle) => {
                let bound_addr = handle.bound_http_addr();
                let url = format!("http://{}", bound_addr);
                *handle_lock = Some(handle);
                Ok(env.new_string(url)?.into_raw())
            }
            Err(err) => {
                let err_msg = format!("Failed to start server: {}", err);
                let jni_err_msg = jni::strings::JNIString::from(err_msg);
                let _ = env.throw_new(jni::jni_str!("java/lang/RuntimeException"), jni_err_msg);
                Ok(std::ptr::null_mut())
            }
        }
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
/// Stops the embedded server from a JVM native call.
///
/// # Safety
///
/// The JNI environment and class handle must originate from the invoking JVM
/// thread.
pub unsafe extern "C" fn Java_com_stremio_mobile_server_JniStreamingServerController_stopServerNative(
    _env: EnvUnowned, _class: JClass,
) {
    let mut handle_lock = SERVER_HANDLE.lock().unwrap();
    if let Some(handle) = handle_lock.take() {
        let _ = handle.shutdown();
        let _ = handle.join();
    }
}

#[unsafe(no_mangle)]
/// Returns the embedded server URL to a JVM native caller.
///
/// # Safety
///
/// The JNI environment and class handle must be valid for the duration of this
/// call and must originate from the invoking JVM thread.
pub unsafe extern "C" fn Java_com_stremio_mobile_server_JniStreamingServerController_getServerUrlNative(
    mut env: EnvUnowned, _class: JClass,
) -> jstring {
    env.with_env(|env| -> jni::errors::Result<jstring> {
        let handle_lock = SERVER_HANDLE.lock().unwrap();
        if let Some(handle) = handle_lock.as_ref() {
            let bound_addr = handle.bound_http_addr();
            let url = format!("http://{}", bound_addr);
            Ok(env.new_string(url)?.into_raw())
        } else {
            Ok(std::ptr::null_mut())
        }
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
