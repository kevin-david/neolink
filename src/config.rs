use lazy_static::lazy_static;
use neolink_core::bc_protocol::{DiscoveryMethods, PrintFormat};
use regex::Regex;
use serde::Deserialize;
use std::clone::Clone;
use validator::{Validate, ValidationError};
use validator_derive::Validate;

lazy_static! {
    static ref RE_STREAM_SRC: Regex =
        Regex::new(r"^(mainStream|subStream|externStream|both|all)$").unwrap();
    static ref RE_TLS_CLIENT_AUTH: Regex = Regex::new(r"^(none|request|require)$").unwrap();
    static ref RE_PAUSE_MODE: Regex = Regex::new(r"^(black|still|test|none)$").unwrap();
    static ref RE_MAXENC_SRC: Regex =
        Regex::new(r"^([nN]one|[Aa][Ee][Ss]|[Bb][Cc][Ee][Nn][Cc][Rr][Yy][Pp][Tt])$").unwrap();
}

#[derive(Debug, Deserialize, Validate, Clone)]
pub(crate) struct Config {
    #[validate]
    pub(crate) cameras: Vec<CameraConfig>,

    #[serde(rename = "bind", default = "default_bind_addr")]
    pub(crate) bind_addr: String,

    #[validate(range(min = 0, max = 65535, message = "Invalid port", code = "bind_port"))]
    #[serde(default = "default_bind_port")]
    pub(crate) bind_port: u16,

    #[serde(default = "default_tokio_console")]
    pub(crate) tokio_console: bool,

    #[serde(default = "default_certificate")]
    pub(crate) certificate: Option<String>,

    #[validate(regex(
        path = "RE_TLS_CLIENT_AUTH",
        message = "Incorrect tls auth",
        code = "tls_client_auth"
    ))]
    #[serde(default = "default_tls_client_auth")]
    pub(crate) tls_client_auth: String,

    #[validate]
    #[serde(default)]
    pub(crate) users: Vec<UserConfig>,
}

#[derive(Debug, Deserialize, Validate, Clone)]
#[validate(schema(function = "validate_camera_config"))]
pub(crate) struct CameraConfig {
    pub(crate) name: String,

    #[serde(rename = "address")]
    pub(crate) camera_addr: Option<String>,

    #[serde(rename = "uid")]
    pub(crate) camera_uid: Option<String>,

    pub(crate) username: String,
    pub(crate) password: Option<String>,

    #[validate(regex(
        path = "RE_STREAM_SRC",
        message = "Incorrect stream source",
        code = "stream"
    ))]
    #[serde(default = "default_stream")]
    pub(crate) stream: String,

    pub(crate) permitted_users: Option<Vec<String>>,

    #[validate(range(min = 0, max = 31, message = "Invalid channel", code = "channel_id"))]
    #[serde(default = "default_channel_id")]
    pub(crate) channel_id: u8,

    #[validate]
    #[serde(default = "default_mqtt")]
    pub(crate) mqtt: Option<MqttConfig>,

    #[validate]
    #[serde(default = "default_pause")]
    pub(crate) pause: PauseConfig,

    #[serde(default = "default_discovery")]
    pub(crate) discovery: DiscoveryMethods,

    #[serde(default = "default_maxenc")]
    #[validate(regex(
        path = "RE_MAXENC_SRC",
        message = "Invalid maximum encryption method",
        code = "max_encryption"
    ))]
    pub(crate) max_encryption: String,

    #[serde(default = "default_strict")]
    /// If strict then the media stream will error in the event that the media packets are not as expected
    pub(crate) strict: bool,

    #[serde(default = "default_print", alias = "print")]
    pub(crate) print_format: PrintFormat,

    #[serde(default = "default_update_time", alias = "time")]
    pub(crate) update_time: bool,

    #[validate(range(
        min = 10,
        max = 500,
        message = "Invalid buffer size",
        code = "buffer_size"
    ))]
    #[serde(default = "default_buffer_size", alias = "size", alias = "buffer")]
    pub(crate) buffer_size: usize,

    #[serde(
        default = "default_smoothing",
        alias = "smoothing",
        alias = "stretching"
    )]
    pub(crate) use_smoothing: bool,
}

#[derive(Debug, Deserialize, Validate, Clone)]
pub(crate) struct UserConfig {
    #[validate(custom = "validate_username")]
    #[serde(alias = "username")]
    pub(crate) name: String,

    #[serde(alias = "password")]
    pub(crate) pass: String,
}

#[derive(Debug, Deserialize, Clone, Validate)]
#[validate(schema(function = "validate_mqtt_config", skip_on_field_errors = true))]
pub(crate) struct MqttConfig {
    #[serde(alias = "server")]
    pub(crate) broker_addr: String,

    pub(crate) port: u16,

    #[serde(default)]
    pub(crate) credentials: Option<(String, String)>,

    #[serde(default)]
    pub(crate) ca: Option<std::path::PathBuf>,

    #[serde(default)]
    pub(crate) client_auth: Option<(std::path::PathBuf, std::path::PathBuf)>,

    #[serde(default)]
    pub(crate) discovery: Option<MqttDiscoveryConfig>,
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub(crate) struct MqttDiscoveryConfig {
    pub(crate) topic: String,

    pub(crate) features: Vec<String>,
}

fn validate_mqtt_config(config: &MqttConfig) -> Result<(), ValidationError> {
    if config.ca.is_some() && config.client_auth.is_some() {
        Err(ValidationError::new(
            "Cannot have both ca and client_auth set",
        ))
    } else {
        Ok(())
    }
}

fn default_mqtt() -> Option<MqttConfig> {
    None
}

fn default_print() -> PrintFormat {
    PrintFormat::None
}

fn default_discovery() -> DiscoveryMethods {
    DiscoveryMethods::Relay
}

fn default_maxenc() -> String {
    "Aes".to_string()
}

#[derive(Debug, Deserialize, Validate, Clone)]
pub(crate) struct PauseConfig {
    #[serde(default = "default_on_motion")]
    pub(crate) on_motion: bool,

    #[serde(default = "default_on_disconnect", alias = "on_client")]
    pub(crate) on_disconnect: bool,

    #[serde(default = "default_motion_timeout", alias = "timeout")]
    pub(crate) motion_timeout: f64,

    #[serde(default = "default_pause_mode")]
    #[validate(regex(
        path = "RE_PAUSE_MODE",
        message = "Incorrect pause mode",
        code = "mode"
    ))]
    pub(crate) mode: String,
}

fn default_bind_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_bind_port() -> u16 {
    8554
}

fn default_stream() -> String {
    "both".to_string()
}

fn default_certificate() -> Option<String> {
    None
}

fn default_tls_client_auth() -> String {
    "none".to_string()
}

fn default_tokio_console() -> bool {
    false
}

fn default_channel_id() -> u8 {
    0
}

fn default_update_time() -> bool {
    false
}

fn default_motion_timeout() -> f64 {
    1.
}

fn default_on_disconnect() -> bool {
    false
}

fn default_on_motion() -> bool {
    false
}

fn default_pause_mode() -> String {
    "none".to_string()
}

fn default_strict() -> bool {
    false
}

fn default_pause() -> PauseConfig {
    PauseConfig {
        on_motion: default_on_motion(),
        on_disconnect: default_on_disconnect(),
        motion_timeout: default_motion_timeout(),
        mode: default_pause_mode(),
    }
}

fn default_smoothing() -> bool {
    true
}

fn default_buffer_size() -> usize {
    100
}

pub(crate) static RESERVED_NAMES: &[&str] = &["anyone", "anonymous"];
fn validate_username(name: &str) -> Result<(), ValidationError> {
    if name.trim().is_empty() {
        return Err(ValidationError::new("username cannot be empty"));
    }
    if RESERVED_NAMES.contains(&name) {
        return Err(ValidationError::new("This is a reserved username"));
    }
    Ok(())
}

fn validate_camera_config(camera_config: &CameraConfig) -> Result<(), ValidationError> {
    match (&camera_config.camera_addr, &camera_config.camera_uid) {
        (None, None) => Err(ValidationError::new(
            "Either camera address or uid must be given",
        )),
        _ => Ok(()),
    }
}
