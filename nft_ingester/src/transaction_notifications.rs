use std::{sync::Arc, time::Duration};

use crate::{
    metric, metrics::capture_result, program_transformers::ProgramTransformer, tasks::TaskData,
};
use cadence_macros::{is_global_default_set, statsd_count, statsd_time};
use chrono::Utc;
use flatbuffers::FlatBufferBuilder;
use log::debug;
use plerkle_messenger::TRANSACTION_STREAM;

use hex::encode;
use sqlx::{Pool, Postgres};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle, time::Instant};

pub fn transaction_worker(
    pool: Pool<Postgres>,
    bg_task_sender: UnboundedSender<TaskData>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let manager = Arc::new(ProgramTransformer::new(pool, bg_task_sender));

        // TODO: do not create it here, use already created receiver
        let receiver = solana_geyser_zmq::receiver::TcpReceiver::new(
            Box::new(move |data| {
                debug!("Received data: {:?}", encode(data.to_vec()));

                let manager_clone = Arc::clone(&manager);
                // TODO: maybe make the callback itself async?
                tokio::spawn(async move {
                    handle_transaction(manager_clone, data[1..].to_vec()).await;
                });
            }),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        receiver.connect("127.0.0.1:3333".parse().unwrap()).unwrap();
    })
}

async fn handle_transaction(manager: Arc<ProgramTransformer>, item: Vec<u8>) -> Option<String> {
    let id = "1".to_string(); // TODO: used only for metrics, probably will be dropped
    let mut ret_id = None;

    if let Ok(tx) = solana_geyser_zmq::flatbuffer::transaction_info_generated::transaction_info::root_as_transaction_info(&item) {
        let signature = tx.signature_string().unwrap_or("NO SIG");
        debug!("Received transaction: {}", signature);
        metric! {
            statsd_count!("ingester.seen", 1, "stream" => TRANSACTION_STREAM);
        }
        let seen_at = Utc::now();
        metric! {
            statsd_time!(
                "ingester.bus_ingest_time",
                seen_at.timestamp_millis() as u64,
                "stream" => TRANSACTION_STREAM
            );
        }

        // map
        let mut builder = FlatBufferBuilder::new();

        let slot_idx = format!("{}_{}", tx.slot(), tx.index().unwrap_or_default());

        let versioned_tx = bincode::deserialize::<solana_sdk::transaction::VersionedTransaction>(tx.transaction().unwrap().bytes()).unwrap();
        let version = match versioned_tx.message {
            solana_sdk::message::VersionedMessage::Legacy(_) => plerkle_serialization::TransactionVersion::Legacy,
            solana_sdk::message::VersionedMessage::V0(_) => plerkle_serialization::TransactionVersion::V0,
        };

        let account_keys = {
            let mut account_keys_fb_vec = vec![];
            for key in tx.account_keys().iter() {
                let pubkey = plerkle_serialization::Pubkey(key.bytes().try_into().unwrap());
                account_keys_fb_vec.push(pubkey);
            }

            if let Some(loaded_addr) = tx.loaded_addresses_string() {
                for i in loaded_addr.writable().iter() {
                    let pubkey = plerkle_serialization::Pubkey(i.bytes().try_into().unwrap());
                    account_keys_fb_vec.push(pubkey);
                }

                for i in loaded_addr.readonly().iter() {
                    let pubkey = plerkle_serialization::Pubkey(i.bytes().try_into().unwrap());
                    account_keys_fb_vec.push(pubkey);
                }
            }

            if !account_keys_fb_vec.is_empty() {
                Some(builder.create_vector(&account_keys_fb_vec))
            } else {
                None
            }
        };

        let log_messages = tx.transaction_meta().and_then(|meta| meta.log_messages()).map(|msgs| {
            let mapped = msgs.iter().map(|msg| builder.create_string(msg) ).collect::<Vec<_>>();
            builder.create_vector(&mapped)
        });

        let outer_instructions = versioned_tx.message.instructions();
        let outer_instructions = if !outer_instructions.is_empty() {
            let mut instructions_fb_vec = Vec::with_capacity(outer_instructions.len());
            for compiled_instruction in outer_instructions.iter() {
                let program_id_index = compiled_instruction.program_id_index;
                let accounts = Some(builder.create_vector(&compiled_instruction.accounts));
                let data = Some(builder.create_vector(&compiled_instruction.data));
                instructions_fb_vec.push(plerkle_serialization::CompiledInstruction::create(
                    &mut builder,
                    &plerkle_serialization::CompiledInstructionArgs {
                        program_id_index,
                        accounts,
                        data,
                    },
                ));
            }
            Some(builder.create_vector(&instructions_fb_vec))
        } else {
            None
        };

        let inner_instructions = if let Some(inner_instructions_vec) = tx.transaction_meta().and_then(|meta| meta.inner_instructions())
        {
            let mut overall_fb_vec = Vec::with_capacity(inner_instructions_vec.len());
            for inner_instructions in inner_instructions_vec.iter() {
                let index = inner_instructions.index();
                if let Some(instructions) = inner_instructions.instructions() {
                    let mut instructions_fb_vec = Vec::with_capacity(instructions.len());
                    for compiled_instruction in instructions.iter() {
                        let program_id_index = compiled_instruction.program_id_index();
                        let accounts = compiled_instruction.accounts().map(|acc| builder.create_vector(acc.bytes()));
                        let data = compiled_instruction.data().map(|data| builder.create_vector(data.bytes()));
                        let compiled = plerkle_serialization::CompiledInstruction::create(
                            &mut builder,
                            &plerkle_serialization::CompiledInstructionArgs {
                                program_id_index,
                                accounts,
                                data,
                            },
                        );
                        instructions_fb_vec.push(plerkle_serialization::CompiledInnerInstruction::create(
                            &mut builder,
                            &plerkle_serialization::CompiledInnerInstructionArgs {
                                compiled_instruction: Some(compiled),
                                stack_height: 0, // Desperatley need this when it comes in 1.15
                            },
                        ));
                    }

                    let instructions = Some(builder.create_vector(&instructions_fb_vec));
                    overall_fb_vec.push(plerkle_serialization::CompiledInnerInstructions::create(
                        &mut builder,
                        &plerkle_serialization::CompiledInnerInstructionsArgs {
                            index,
                            instructions,
                        },
                    ))
                }
            }

            Some(builder.create_vector(&overall_fb_vec))
        } else {
            None
        };

        let args = plerkle_serialization::TransactionInfoArgs {
            is_vote: tx.is_vote(),
            account_keys,
            log_messages,
            inner_instructions: None,
            outer_instructions,
            slot: tx.slot(),
            slot_index: Some(builder.create_string(&slot_idx)),
            seen_at: seen_at.timestamp_millis(),
            signature: Some(builder.create_string(&signature)),
            compiled_inner_instructions: inner_instructions,
            version,
        };
        let transaction_info_wip = plerkle_serialization::TransactionInfo::create(&mut builder, &args);
        builder.finish(transaction_info_wip, None);
        let transaction_info = plerkle_serialization::root_as_transaction_info(builder.finished_data()).unwrap();

        dbg!(transaction_info);

        let begin = Instant::now();
        let res = manager.handle_transaction(&transaction_info).await;
        let should_ack = capture_result(
            id.clone(),
            TRANSACTION_STREAM,
            ("txn", "txn"),
            1, // TODO: here was "item.tries". that's for metrics, so we can ignore it for now
            res,
            begin,
            tx.signature_string(),
            None,
        );
        if should_ack {
            ret_id = Some(id);
        }
    }
    ret_id
}
