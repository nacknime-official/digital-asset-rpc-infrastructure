use core::time;
use figment::{
    providers::{Env, Format, Yaml},
    value::Value,
    Figment,
};
use plerkle_messenger::MessengerConfig;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::Deserialize;
use std::{
    env,
    fmt::{Display, Formatter},
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
};
use tracing_subscriber::fmt;

use crate::{error::IngesterError, tasks::BackgroundTaskRunnerConfig};

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct IngesterConfig {
    pub database_config: DatabaseConfig,
    pub messenger_config: MessengerConfig,
    pub tcp_config: TcpConfig,
    pub env: Option<String>,
    pub rpc_config: RpcConfig,
    pub big_table_config: BigTableConfig,
    pub metrics_port: Option<u16>,
    pub metrics_host: Option<String>,
    pub backfiller: Option<bool>,
    pub role: Option<IngesterRole>,
    pub max_postgres_connections: Option<u32>,
    pub account_stream_worker_count: Option<u32>,
    pub transaction_stream_worker_count: Option<u32>,
    pub code_version: Option<&'static str>,
    pub background_task_runner_config: Option<BackgroundTaskRunnerConfig>,
}

impl IngesterConfig {
    /// Get the db url out of the dict, this is built a a dict so that future extra db parameters can be easily shoved in.
    /// this panics if the key is not present
    pub fn get_database_url(&self) -> String {
        self.database_config
            .get(DATABASE_URL_KEY)
            .and_then(|u| u.clone().into_string())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("Database connection string missing: {}", DATABASE_URL_KEY),
            })
            .unwrap()
    }

    pub fn get_rpc_url(&self) -> String {
        self.rpc_config
            .get(RPC_URL_KEY)
            .and_then(|u| u.clone().into_string())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("RPC connection string missing: {}", RPC_URL_KEY),
            })
            .unwrap()
    }

    pub fn get_messneger_client_config(&self) -> MessengerConfig {
        let mut mc = self.messenger_config.clone();
        mc.connection_config
            .insert("consumer_id".to_string(), Value::from(rand_string()));
        mc
    }

    pub fn get_account_stream_worker_count(&self) -> u32 {
        self.account_stream_worker_count.unwrap_or(2)
    }

    pub fn get_transaction_stream_worker_count(&self) -> u32 {
        self.transaction_stream_worker_count.unwrap_or(2)
    }

    pub fn get_tcp_receiver_connect_timeout(&self, backfiller: bool) -> time::Duration {
        let key = match backfiller {
            false => TCP_RECEIVER_CONNECT_TIMEOUT_KEY,
            true => TCP_RECEIVER_BACKFILLER_CONNECT_TIMEOUT_KEY,
        };
        time::Duration::from_secs(
            self.tcp_config
                .get(key)
                .and_then(|a| a.to_u128())
                .ok_or(IngesterError::ConfigurationError {
                    msg: format!("TCP receiver connect timeout missing: {}", key),
                })
                .unwrap() as u64,
        )
    }

    pub fn get_tcp_receiver_reconnect_interval(&self, backfiller: bool) -> time::Duration {
        let key = match backfiller {
            false => TCP_RECEIVER_RECONNECT_INTERVAL,
            true => TCP_RECEIVER_BACKFILLER_RECONNECT_INTERVAL,
        };
        time::Duration::from_secs(
            self.tcp_config
                .get(key)
                .and_then(|a| a.to_u128())
                .ok_or(IngesterError::ConfigurationError {
                    msg: format!("TCP receiver reconnect interval missing: {}", key),
                })
                .unwrap() as u64,
        )
    }

    pub fn get_tcp_receiver_addr(&self, backfiller: bool) -> SocketAddr {
        let key = match backfiller {
            false => TCP_RECEIVER_ADDR,
            true => TCP_RECEIVER_BACKFILLER_ADDR,
        };
        self.tcp_config
            .get(key)
            .and_then(|a| a.as_str())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("TCP receiver address missing: {}", key),
            })
            .unwrap()
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap()
    }

    pub fn get_tcp_sender_backfiller_port(&self) -> u16 {
        self.tcp_config
            .get(TCP_SENDER_BACKFILLER_PORT)
            .and_then(|a| a.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "TCP sender port for backfiller missing: {}",
                    TCP_SENDER_BACKFILLER_PORT
                ),
            })
            .unwrap() as u16
    }

    pub fn get_tcp_sender_backfiller_buffer_size(&self) -> usize {
        self.tcp_config
            .get(TCP_SENDER_BACKFILLER_BUFFER_SIZE)
            .and_then(|a| a.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "TCP sender buffer size for backfiller missing: {}",
                    TCP_SENDER_BACKFILLER_BUFFER_SIZE
                ),
            })
            .unwrap() as usize
    }

    pub fn get_tcp_sender_backfiller_batch_max_bytes(&self) -> usize {
        self.tcp_config
            .get(TCP_SENDER_BACKFILLER_BATCH_MAX_BYTES)
            .and_then(|a| a.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "TCP sender batch max bytes for backfiller missing: {}",
                    TCP_SENDER_BACKFILLER_BATCH_MAX_BYTES
                ),
            })
            .unwrap() as usize
    }
}

// Types and constants used for Figment configuration items.
pub type DatabaseConfig = figment::value::Dict;

pub const DATABASE_URL_KEY: &str = "url";
pub const DATABASE_LISTENER_CHANNEL_KEY: &str = "listener_channel";

pub type RpcConfig = figment::value::Dict;
pub type BigTableConfig = figment::value::Dict;

pub const RPC_URL_KEY: &str = "url";
pub const RPC_COMMITMENT_KEY: &str = "commitment";
pub const CODE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RPC_TIMEOUT_KEY: &str = "timeout";

pub const BIG_TABLE_CREDS_KEY: &str = "creds";
pub const BIG_TABLE_TIMEOUT_KEY: &str = "timeout";

pub type TcpConfig = figment::value::Dict;

pub const TCP_RECEIVER_ADDR: &str = "receiver_addr";
pub const TCP_RECEIVER_CONNECT_TIMEOUT_KEY: &str = "receiver_connect_timeout";
pub const TCP_RECEIVER_RECONNECT_INTERVAL: &str = "receiver_reconnect_interval";

pub const TCP_RECEIVER_BACKFILLER_ADDR: &str = "backfiller_receiver_addr";
pub const TCP_RECEIVER_BACKFILLER_CONNECT_TIMEOUT_KEY: &str = "backfiller_receiver_connect_timeout";
pub const TCP_RECEIVER_BACKFILLER_RECONNECT_INTERVAL: &str =
    "backfiller_receiver_reconnect_interval";
pub const TCP_SENDER_BACKFILLER_PORT: &str = "backfiller_sender_port";
pub const TCP_SENDER_BACKFILLER_BATCH_MAX_BYTES: &str = "backfiller_sender_batch_max_bytes";
pub const TCP_SENDER_BACKFILLER_BUFFER_SIZE: &str = "backfiller_sender_buffer_size";

#[derive(Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum IngesterRole {
    All,
    Backfiller,
    BackgroundTaskRunner,
    Ingester,
}

impl Default for IngesterRole {
    fn default() -> Self {
        IngesterRole::All
    }
}

impl Display for IngesterRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngesterRole::All => write!(f, "All"),
            IngesterRole::Backfiller => write!(f, "Backfiller"),
            IngesterRole::BackgroundTaskRunner => write!(f, "BackgroundTaskRunner"),
            IngesterRole::Ingester => write!(f, "Ingester"),
        }
    }
}

pub fn rand_string() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect()
}

pub fn setup_config(config_file: Option<&PathBuf>) -> IngesterConfig {
    let mut figment = Figment::new().join(Env::prefixed("INGESTER_"));

    if let Some(config_file) = config_file {
        figment = figment.join(Yaml::file(config_file));
    }

    let mut config: IngesterConfig = figment
        .extract()
        .map_err(|config_error| IngesterError::ConfigurationError {
            msg: format!("{}", config_error),
        })
        .unwrap();
    config.code_version = Some(CODE_VERSION);
    config
}

pub fn init_logger() {
    let env_filter = env::var("RUST_LOG")
        .or::<Result<String, ()>>(Ok("info".to_string()))
        .unwrap();
    let t = tracing_subscriber::fmt().with_env_filter(env_filter);
    t.event_format(fmt::format::json()).init();
}
