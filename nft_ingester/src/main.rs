mod backfiller;
mod error;
mod metrics;
mod program_transformers;
mod tasks;
use crate::{
    backfiller::backfiller,
    error::IngesterError,
    program_transformers::ProgramTransformer,
    tasks::{common::task::DownloadMetadataTask, BgTask, TaskData, TaskManager},
};
use blockbuster::instruction::{order_instructions, InstructionBundle, IxPair};
use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::is_global_default_set;
use cadence_macros::{set_global_default, statsd_count, statsd_gauge, statsd_time};
use chrono::Utc;
use figment::{providers::Env, value::Value, Figment};
use futures::{stream::FuturesUnordered, StreamExt};
use plerkle_messenger::ConsumptionType;
use plerkle_messenger::{
    redis_messenger::RedisMessenger, Messenger, MessengerConfig, RecvData, ACCOUNT_STREAM,
    TRANSACTION_STREAM,
};
use plerkle_serialization::{root_as_account_info, root_as_transaction_info, Pubkey as FBPubkey};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::Deserialize;
use sqlx::{self, postgres::PgPoolOptions, Pool, Postgres};
use std::fmt::{Display, Formatter};
use std::net::UdpSocket;
use std::sync::Arc;
use tokio::{
    sync::mpsc::UnboundedSender,
    task::JoinSet,
    time::{self, Instant},
};

// Types and constants used for Figment configuration items.
pub type DatabaseConfig = figment::value::Dict;

pub const DATABASE_URL_KEY: &str = "url";
pub const DATABASE_LISTENER_CHANNEL_KEY: &str = "listener_channel";

pub type RpcConfig = figment::value::Dict;
pub type BigTableConfig = figment::value::Dict;

pub const RPC_URL_KEY: &str = "url";
pub const RPC_COMMITMENT_KEY: &str = "commitment";
pub const RPC_TIMEOUT_KEY: &str = "timeout";

pub const BIG_TABLE_CREDS_KEY: &str = "creds";
pub const BIG_TABLE_TIMEOUT_KEY: &str = "timeout";

#[derive(Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum IngesterRole {
    All,
    Backfiller,
    BackgroundTaskRunner,
    Ingester,
}

impl Display for IngesterRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngesterRole::All => write!(f, "all"),
            IngesterRole::Backfiller => write!(f, "backfiller"),
            IngesterRole::BackgroundTaskRunner => write!(f, "background_task_runner"),
            IngesterRole::Ingester => write!(f, "ingester"),
        }
    }
}

// Struct used for Figment configuration items.
#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct IngesterConfig {
    pub database_config: DatabaseConfig,
    pub messenger_config: MessengerConfig,
    pub env: Option<String>,
    pub rpc_config: RpcConfig,
    pub big_table_config: BigTableConfig,
    pub metrics_port: Option<u16>,
    pub metrics_host: Option<String>,
    pub backfiller: Option<bool>,
    pub role: Option<IngesterRole>,
    pub max_postgres_connections: Option<u32>,
}

fn setup_metrics(config: &IngesterConfig) {
    let uri = config.metrics_host.clone();
    let port = config.metrics_port;
    let env = config.env.clone().unwrap_or_else(|| "dev".to_string());
    if uri.is_some() || port.is_some() {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let host = (uri.unwrap(), port.unwrap());
        let udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
        let queuing_sink = QueuingMetricSink::from(udp_sink);
        let cons = config
            .messenger_config
            .connection_config
            .get("consumer_id")
            .unwrap()
            .as_str()
            .unwrap();
        let builder = StatsdClient::builder("das_ingester", queuing_sink);
        let client = builder
            .with_tag("env", env)
            .with_tag("consumer_id", cons)
            .build();
        set_global_default(client);
    }
}

fn rand_string() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect()
}

#[tokio::main]
async fn main() {
    // Read config.
    println!("Starting DASgester");
    let mut config: IngesterConfig = Figment::new()
        .join(Env::prefixed("INGESTER_"))
        .extract()
        .map_err(|config_error| IngesterError::ConfigurationError {
            msg: format!("{}", config_error),
        })
        .unwrap();
    config
        .messenger_config
        .connection_config
        .insert("consumer_id".to_string(), Value::from(rand_string()));

    let url = config
        .database_config
        .get(DATABASE_URL_KEY)
        .and_then(|u| u.clone().into_string())
        .ok_or(IngesterError::ConfigurationError {
            msg: format!("Database connection string missing: {}", DATABASE_URL_KEY),
        })
        .unwrap();

    setup_metrics(&config);

    let pool = PgPoolOptions::new()
        .max_connections(config.max_postgres_connections.unwrap_or(100))
        .connect(&url)
        .await
        .unwrap();

    let backfiller = backfiller::<RedisMessenger>(pool.clone(), config.clone());
    let role = config.role.unwrap_or(IngesterRole::All);
    let bg_task_definitions: Vec<Box<dyn BgTask>> = vec![Box::new(DownloadMetadataTask {})];
    let mut background_task_manager =
        TaskManager::new(rand_string(), pool.clone(), bg_task_definitions);
    let background_task_manager_handle = background_task_manager
        .start_listener(role == IngesterRole::BackgroundTaskRunner || role == IngesterRole::All);
    let backgroun_task_sender = background_task_manager.get_sender().unwrap();

    let txn_stream = service_transaction_stream::<RedisMessenger>(
        pool.clone(),
        backgroun_task_sender.clone(), // This is allowed because we must
        config.messenger_config.clone(),
    );

    let account_stream = service_account_stream::<RedisMessenger>(
        pool.clone(),
        backgroun_task_sender,
        config.messenger_config.clone(),
    );

    let mut tasks = JoinSet::new();

    let stream_size_timer = async move {
        let mut interval = time::interval(tokio::time::Duration::from_secs(10));
        let mut messenger = RedisMessenger::new(config.messenger_config.clone())
            .await
            .unwrap();
        loop {
            interval.tick().await;

            let tx_size = messenger.stream_size(TRANSACTION_STREAM).await;
            let acc_size = messenger.stream_size(ACCOUNT_STREAM).await;
            if tx_size.is_err() {
                metric! {
                    statsd_count!("ingester.transaction_stream_size_error", 1);
                }
            }
            if acc_size.is_err() {
                metric! {
                    statsd_count!("ingester.account_stream_size_error", 1);
                }
            }
            let tx_size = tx_size.unwrap_or(0);
            let acc_size = acc_size.unwrap_or(0);
            metric! {
                statsd_gauge!("ingester.transaction_stream_size", tx_size);
                statsd_gauge!("ingester.account_stream_size", acc_size);
            }
        }
    };

    match role {
        IngesterRole::All => {
            tasks.spawn(backfiller.await);
            tasks.spawn(txn_stream.await);
            tasks.spawn(account_stream.await);
            tasks.spawn(background_task_manager_handle);
            tasks.spawn(background_task_manager.start_runner());
            tasks.spawn(stream_size_timer);
        }
        IngesterRole::Backfiller => {
            tasks.spawn(backfiller.await);
        }
        IngesterRole::BackgroundTaskRunner => {
            tasks.spawn(background_task_manager.start_runner());
            tasks.spawn(stream_size_timer);
        }
        IngesterRole::Ingester => {
            tasks.spawn(background_task_manager_handle);
            tasks.spawn(txn_stream.await);
            tasks.spawn(account_stream.await);
        }
    }
    let roles_str = role.to_string();
    metric! {
        statsd_count!("ingester.startup", 1, "role" => &roles_str);
    }

    // Wait for ctrl-c.
    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            println!("Unable to listen for shutdown signal: {}", err);
        }
    }

    tasks.shutdown().await;
}

async fn service_transaction_stream<T: Messenger>(
    pool: Pool<Postgres>,
    tasks: UnboundedSender<TaskData>,
    messenger_config: MessengerConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let pool_cloned = pool.clone();
            let tasks_cloned = tasks.clone();
            let messenger_config_cloned = messenger_config.clone();

            let result = tokio::spawn(async {
                let manager = Arc::new(ProgramTransformer::new(pool_cloned, tasks_cloned));
                let mut messenger = T::new(messenger_config_cloned).await.unwrap();
                println!("Setting up transaction listener");

                loop {
                    if let Ok(data) = messenger
                        .recv(TRANSACTION_STREAM, ConsumptionType::All)
                        .await
                    {
                        let ids = handle_transaction(&manager, data).await;
                        if !ids.is_empty() {
                            if let Err(e) = messenger.ack_msg(TRANSACTION_STREAM, &ids).await {
                                println!("Error ACK-ing messages {:?}", e);
                            }
                        }
                    }
                }
            })
            .await;

            match result {
                Ok(_) => break,
                Err(err) if err.is_panic() => {
                    statsd_count!("ingester.service_transaction_stream.task_panic", 1);
                }
                Err(err) => {
                    let err = err.to_string();
                    statsd_count!("ingester.service_transaction_stream.task_error", 1, "error" => &err);
                }
            }
        }
    })
}

async fn service_account_stream<T: Messenger>(
    pool: Pool<Postgres>,
    tasks: UnboundedSender<TaskData>,
    messenger_config: MessengerConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let pool_cloned = pool.clone();
            let tasks_cloned = tasks.clone();
            let messenger_config_cloned = messenger_config.clone();
            let result = tokio::spawn(async {
                let manager = Arc::new(ProgramTransformer::new(pool_cloned, tasks_cloned));
                let mut messenger = T::new(messenger_config_cloned).await.unwrap();
                println!("Setting up account listener");
                loop {
                    let mc = manager.clone();
                    let rc = messenger.recv(ACCOUNT_STREAM, ConsumptionType::All).await;
                    match rc {
                        Ok(data) => {
                            let dl = data.len();
                            if dl > 0 {
                                metric! {
                                    statsd_count!("ingester.account_entries_claimed", dl as i64);
                                }
                                let s = Instant::now();

                                let ids = handle_account(&mc, data).await;
                                metric! {
                                    statsd_time!("ingester.wall_batch_time", s.elapsed());
                                }
                                if !ids.is_empty() {
                                    if let Err(e) = messenger.ack_msg(ACCOUNT_STREAM, &ids).await {
                                        println!("Error ACK-ing messages {:?}", e);
                                    } else {
                                        metric! {
                                            statsd_count!("ingester.account_entries_acked", ids.len() as i64);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error receiving messages {:?}", e);
                        }
                    }
                }
            })
            .await;

            match result {
                Ok(_) => break,
                Err(err) if err.is_panic() => {
                    statsd_count!("ingester.service_account_stream.task_panic", 1);
                }
                Err(err) => {
                    let err = err.to_string();
                    statsd_count!("ingester.service_account_stream.task_error", 1, "error" => &err);
                }
            }
        }
    })
}

async fn handle_account(manager: &Arc<ProgramTransformer>, data: Vec<RecvData>) -> Vec<String> {
    metric! {
        statsd_gauge!("ingester.account_batch_size", data.len() as u64);
    }

    let tasks = FuturesUnordered::new();
    for item in data.into_iter() {
        let manager = Arc::clone(manager);

        tasks.push(async move {
            let id = item.id;
            let mut ret_id = None;
            if item.tries > 0 {
                metric! {
                    statsd_count!("ingester.account_stream_redelivery", 1);
                }
            }

            let data = item.data;
            // Get root of account info flatbuffers object.
            if let Ok(account_update) = root_as_account_info(&data) {

            let seen_at = Utc::now();
            let str_program_id =
                bs58::encode(account_update.owner().unwrap().0.as_slice()).into_string();
            metric! {
                statsd_count!("ingester.account_update_seen", 1, "owner" => &str_program_id);
                statsd_time!(
                    "ingester.account_bus_ingest_time",
                    (seen_at.timestamp_millis() - account_update.seen_at()) as u64,
                    "owner" => &str_program_id
                );
            }
            let begin_processing = Instant::now();
            let res = manager.handle_account_update(account_update).await;
            match res {
                Ok(_) => {
                    if item.tries == 0 {
                        metric! {
                            statsd_time!("ingester.account_proc_time", begin_processing.elapsed().as_millis() as u64, "owner" => &str_program_id);
                        }
                        metric! {
                            statsd_count!("ingester.account_update_success", 1, "owner" => &str_program_id);
                        }
                    }
                    ret_id = Some(id);
                }
                Err(err) if err == IngesterError::NotImplemented => {
                    metric! {
                        statsd_count!("ingester.account_not_implemented", 1, "owner" => &str_program_id, "error" => "ni");
                    }
                    ret_id = Some(id);
                }
                Err(IngesterError::DeserializationError(_)) => {
                    metric! {
                        statsd_count!("ingester.account_update_error", 1, "owner" => &str_program_id, "error" => "de");
                    }
                    ret_id = Some(id);
                }
                Err(IngesterError::ParsingError(_)) => {
                    metric! {
                        statsd_count!("ingester.account_update_error", 1, "owner" => &str_program_id, "error" => "parse");
                    }
                    ret_id = Some(id);
                }
                Err(err) => {
                    println!("Error handling account update: {:?}", err);
                    metric! {
                        statsd_count!("ingester.account_update_error", 1, "owner" => &str_program_id, "error" => "u");
                    }
                }

            }
        }
            ret_id
        });
    }
    tasks
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect()
}

async fn handle_transaction(manager: &Arc<ProgramTransformer>, data: Vec<RecvData>) -> Vec<String> {
    metric! {
        statsd_gauge!("ingester.txn_batch_size", data.len() as u64);
    }

    let tasks = FuturesUnordered::new();
    for item in data {
        let manager = Arc::clone(manager);
        tasks.push(async move {
        let mut ret_id = None;
        if item.tries > 0 {
            metric! {
                statsd_count!("ingester.tx_stream_redelivery", 1);
            }
        }
        let id = item.id.to_string();
        let tx_data = item.data;
        if let Ok(tx) = root_as_transaction_info(&tx_data) {
            let signature = tx.signature().unwrap_or("NO SIG");
            if let Some(si) = tx.slot_index() {
                let slt_idx = format!("{}-{}", tx.slot(), si);
                metric! {
                    statsd_count!("ingester.transaction_event_seen", 1, "slot-idx" => &slt_idx);
                }
            }
            let seen_at = Utc::now();
            metric! {
                statsd_time!(
                    "ingester.bus_ingest_time",
                    (seen_at.timestamp_millis() - tx.seen_at()) as u64
                );
            }
            let begin = Instant::now();
            let res = manager.handle_transaction(&tx).await;
            match res {
                Ok(_) => {
                    if item.tries == 0 {
                        metric! {
                            statsd_time!("ingester.tx_proc_time", begin.elapsed().as_millis() as u64);
                            statsd_count!("ingester.tx_ingest_success", 1);
                        }
                    } else {
                        metric! {
                            statsd_count!("ingester.tx_ingest_redeliver_success", 1);
                        }
                    }
                    ret_id = Some(id);
                }
                Err(err) if err == IngesterError::NotImplemented => {
                    metric! {
                        statsd_count!("ingester.tx_not_implemented", 1);
                    }
                    ret_id = Some(id);
                }
                Err(err) => {
                    println!("ERROR:txn: {:?} {:?}", signature, err);
                    metric! {
                        statsd_count!("ingester.tx_ingest_error", 1);
                    }
                }
            }
        }
        ret_id
    });
    }
    tasks
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect()
}
