use std::{str::FromStr, sync::Arc, time::Duration};

use crate::{
    metric, metrics::capture_result, program_transformers::ProgramTransformer, tasks::TaskData,
};
use cadence_macros::{is_global_default_set, statsd_count, statsd_time};
use chrono::Utc;
use flatbuffers::FlatBufferBuilder;
use log::{debug, info};
use plerkle_messenger::ACCOUNT_STREAM;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Pool, Postgres};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle, time::Instant};

pub fn account_worker(
    pool: Pool<Postgres>,
    bg_task_sender: UnboundedSender<TaskData>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let manager = Arc::new(ProgramTransformer::new(pool, bg_task_sender));

        // TODO: do not create it here, use already created receiver
        let receiver = solana_geyser_zmq::receiver::TcpReceiver::new(
            Box::new(move |data| {
                debug!("Received data: {:?}", data);

                let manager_clone = Arc::clone(&manager);
                // TODO: maybe make the callback itself async?
                tokio::spawn(async move {
                    handle_account(manager_clone, data[1..].to_vec()).await;
                });
            }),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        receiver.connect("127.0.0.1:3333".parse().unwrap()).unwrap();
    })

    // commented old code for quick reference.
    // TODO: delete
    // tokio::spawn(async move {
    //     let source = T::new(config).await;
    //     if let Ok(mut msg) = source {
    //         let manager = Arc::new(ProgramTransformer::new(pool, bg_task_sender));
    //         loop {
    //             let e = msg.recv(ACCOUNT_STREAM, consumption_type.clone()).await;
    //             let mut tasks = JoinSet::new();
    //             match e {
    //                 Ok(data) => {
    //                     let len = data.len();
    //                     for item in data {
    //                         tasks.spawn(handle_account(Arc::clone(&manager), item));
    //                     }
    //                     if len > 0 {
    //                         debug!("Processed {} accounts", len);
    //                     }
    //                 }
    //                 Err(e) => {
    //                     error!("Error receiving from account stream: {}", e);
    //                     metric! {
    //                         statsd_count!("ingester.stream.receive_error", 1, "stream" => ACCOUNT_STREAM);
    //                     }
    //                 }
    //             }
    //             while let Some(res) = tasks.join_next().await {
    //                 if let Ok(id) = res {
    //                     if let Some(id) = id {
    //                         let send = ack_channel.send((ACCOUNT_STREAM, id));
    //                         if let Err(err) = send {
    //                             metric! {
    //                                 error!("Account stream ack error: {}", err);
    //                                 statsd_count!("ingester.stream.ack_error", 1, "stream" => ACCOUNT_STREAM);
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // })
}

async fn handle_account(manager: Arc<ProgramTransformer>, item: Vec<u8>) -> Option<String> {
    let id = "1".to_string(); // TODO: used only for metrics, probably will be dropped
    let mut ret_id = None;

    if let Ok(account_update) =
        solana_geyser_zmq::flatbuffer::account_info_generated::account_info::root_as_account_info(
            &item,
        )
    {
        info!("here");
        let str_program_id = account_update.owner().unwrap();

        metric! {
            statsd_count!("ingester.seen", 1, "owner" => &str_program_id, "stream" => ACCOUNT_STREAM);
            let seen_at = Utc::now();
            statsd_time!(
                "ingester.bus_ingest_time",
                seen_at.timestamp_millis() as u64,
                "owner" => &str_program_id,
                "stream" => ACCOUNT_STREAM
            );
        }

        // TODO: how to deal with these unwraps?
        // maybe return errors form this func and log them at higher level?
        let account_data = solana_geyser_zmq::flatbuffer::account_data_generated::account_data::root_as_account_data(
            account_update.account_data().unwrap().bytes(),
        ).unwrap();
        let pubkey = Pubkey::from_str(account_update.pubkey().unwrap())
            .unwrap()
            .to_bytes();
        let owner = Pubkey::from_str(str_program_id).unwrap().to_bytes();

        let mut builder = FlatBufferBuilder::new();
        let data = builder.create_vector(account_data.data().unwrap().bytes().as_ref());
        let account_info_wip = plerkle_serialization::AccountInfo::create(
            &mut builder,
            &plerkle_serialization::AccountInfoArgs {
                pubkey: Some(&plerkle_serialization::Pubkey::new(&pubkey)),
                lamports: account_data.lamports(),
                owner: Some(&plerkle_serialization::Pubkey::new(&owner)),
                executable: account_data.executable(),
                rent_epoch: account_data.rent_epoch(),
                data: Some(data),
                write_version: account_data.version(),
                slot: account_update.slot(),
                is_startup: false, // TODO: there's no is_startup in our flatbuffers, so use
                // default value
                seen_at: Utc::now().timestamp_millis(), // TODO: ok or not? I think it'll be better
                                                        // if we add this field to our AccountInfo
                                                        // flatbuffer and let the geyser plugin
                                                        // fill it
            },
        );
        builder.finish(account_info_wip, None);
        let account_info =
            plerkle_serialization::root_as_account_info(builder.finished_data()).unwrap();

        let mut account = None;
        if let Some(pubkey) = account_update.pubkey() {
            account = Some(pubkey.to_string());
        }
        let begin_processing = Instant::now();
        let res = manager.handle_account_update(account_info).await;
        let should_ack = capture_result(
            id.clone(),
            ACCOUNT_STREAM,
            ("owner", &str_program_id),
            1, // TODO: here was "item.tries". that's for metrics, so we can ignore it for now
            res,
            begin_processing,
            None,
            account,
        );
        if should_ack {
            ret_id = Some(id);
        }
    }
    ret_id
}
