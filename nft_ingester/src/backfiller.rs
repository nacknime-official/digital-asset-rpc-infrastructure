//! Backfiller that fills gaps in trees by detecting gaps in sequence numbers
//! in the `backfill_items` table.  Inspired by backfiller.ts/backfill.ts.
use crate::{
    error::IngesterError, IngesterConfig, BIG_TABLE_CREDS_KEY, BIG_TABLE_TIMEOUT_KEY,
    DATABASE_LISTENER_CHANNEL_KEY, RPC_TIMEOUT_KEY, RPC_URL_KEY,
};
use borsh::BorshDeserialize;
use cadence_macros::{statsd_count, statsd_gauge};
use chrono::Utc;
use digital_asset_types::dao::backfill_items;
use flatbuffers::FlatBufferBuilder;
use futures::stream::FuturesUnordered;
use plerkle_messenger::{Messenger, TRANSACTION_STREAM};
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use sea_orm::{
    entity::*, query::*, sea_query::Expr, DatabaseConnection, DbBackend, DbErr, FromQueryResult,
    SqlxPostgresConnector};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{
    account::Account,
    pubkey::Pubkey,
    signature::Signature,
    slot_history::Slot,
};
use solana_sdk_macro::pubkey;
use solana_storage_bigtable::LedgerStorage;
use solana_transaction_status::{
    ConfirmedBlock, EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use spl_account_compression::state::ConcurrentMerkleTreeHeader;
use sqlx::{self, postgres::PgListener, Pool, Postgres};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use stretto::{AsyncCache, AsyncCacheBuilder};
use tokio::{
    sync::Semaphore,
    time::{self, sleep, Duration},
};
// Number of tries to backfill a single tree before marking as "failed".
const NUM_TRIES: i32 = 5;
const TREE_SYNC_INTERVAL: u64 = 60;
const MAX_BACKFILL_CHECK_WAIT: u64 = 5000;
// Constants used for varying delays when failures occur.
const INITIAL_FAILURE_DELAY: u64 = 100;
const MAX_FAILURE_DELAY_MS: u64 = 10_000;
const BLOCK_CACHE_SIZE: usize = 300_000;
const MAX_CACHE_COST: i64 = 32;
const BLOCK_CACHE_DURATION: u64 = 172800;
// Account key used to determine if transaction is a simple vote.
const VOTE: &str = "Vote111111111111111111111111111111111111111";
pub const BUBBLEGUM_SIGNER: Pubkey = pubkey!("4ewWZC5gT6TGpm5LZNDs9wVonfUT2q5PP5sc9kVbwMAK");
/// Main public entry point for backfiller task.
pub async fn backfiller<T: Messenger>(
    pool: Pool<Postgres>,
    config: IngesterConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let pool_cloned = pool.clone();
            let config_cloned = config.clone();
            let tasks = FuturesUnordered::new();
            let block_cache = Arc::new(
                AsyncCacheBuilder::new(BLOCK_CACHE_SIZE, MAX_CACHE_COST)
                    .set_ignore_internal_cost(true)
                    .finalize(tokio::spawn)
                    .expect("failed to create cache"),
            );
            let bc = Arc::clone(&block_cache);
            tasks.push(tokio::spawn(async move {
                println!("Backfiller task running");

                let mut backfiller = Backfiller::<T>::new(pool_cloned, config_cloned, &bc).await;
                backfiller.run_filler().await;
            }));

            let pool_cloned = pool.clone();
            let config_cloned = config.clone();
            let bc = Arc::clone(&block_cache);
            tasks.push(tokio::spawn(async move {
                println!("Backfiller task running");

                let mut backfiller = Backfiller::<T>::new(pool_cloned, config_cloned, &bc).await;
                backfiller.run_finder().await;
            }));

            for task in tasks {
                let res = task.await;
                match res {
                    Ok(_) => break,
                    Err(err) if err.is_panic() => {
                        statsd_count!("ingester.backfiller.task_panic", 1);
                    }
                    Err(err) => {
                        let err = err.to_string();
                        statsd_count!("ingester.backfiller.task_error", 1, "error" => &err);
                    }
                }
            }
        }
    })
}

/// Struct used when querying for unique trees.
#[derive(Debug, FromQueryResult)]
struct UniqueTree {
    tree: Vec<u8>,
}

/// Struct used when querying for unique trees.
#[derive(Debug, FromQueryResult)]
struct TreeWithSlot {
    tree: Vec<u8>,
    slot: i64,
}

#[derive(Debug, Default, Clone)]
struct MissingTree {
    tree: Pubkey,
    slot: u64,
}

/// Struct used when storing trees to backfill.
struct BackfillTree {
    unique_tree: UniqueTree,
    backfill_from_seq_1: bool,
    slot: u64,
}

impl BackfillTree {
    fn new(unique_tree: UniqueTree, backfill_from_seq_1: bool, slot: u64) -> Self {
        Self {
            unique_tree,
            backfill_from_seq_1,
            slot,
        }
    }
}

/// Struct used when querying the max sequence number of a tree.
#[derive(Debug, FromQueryResult, Clone)]
struct MaxSeqItem {
    seq: i64,
}

/// Struct used when querying for items to backfill.
#[derive(Debug, FromQueryResult, Clone)]
struct SimpleBackfillItem {
    seq: i64,
    slot: i64,
}

/// Struct used to store sequence number gap info for a given tree.
#[derive(Debug)]
struct GapInfo {
    prev: SimpleBackfillItem,
    curr: SimpleBackfillItem,
}

impl GapInfo {
    fn new(prev: SimpleBackfillItem, curr: SimpleBackfillItem) -> Self {
        Self { prev, curr }
    }
}

/// Main struct used for backfiller task.
struct Backfiller<'a, T: Messenger> {
    db: DatabaseConnection,
    listener: PgListener,
    rpc_client: RpcClient,
    big_table_client: LedgerStorage,
    messenger: T,
    failure_delay: u64,
    cache: &'a AsyncCache<String, ConfirmedBlock>,
}

impl<'a, T: Messenger> Backfiller<'a, T> {
    /// Create a new `Backfiller` struct.
    async fn new(
        pool: Pool<Postgres>,
        config: IngesterConfig,
        cache: &'a AsyncCache<String, ConfirmedBlock>,
    ) -> Backfiller<'a, T> {
        // Create Sea ORM database connection used later for queries.
        let db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());

        // Connect to database using sqlx and create PgListener.
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool.clone())
            .await
            .map_err(|e| IngesterError::StorageListenerError {
                msg: format!("Could not connect to db for PgListener {e}"),
            })
            .unwrap();

        // Get database listener channel.
        let channel = config
            .database_config
            .get(DATABASE_LISTENER_CHANNEL_KEY)
            .and_then(|u| u.clone().into_string())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "Database listener channel missing: {}",
                    DATABASE_LISTENER_CHANNEL_KEY
                ),
            })
            .unwrap();

        // Setup listener on channel.
        listener
            .listen(&channel)
            .await
            .map_err(|e| IngesterError::StorageListenerError {
                msg: format!("Error listening to channel on backfill_items tbl {e}"),
            })
            .unwrap();

        // Get RPC URL.
        let rpc_url = config
            .rpc_config
            .get(RPC_URL_KEY)
            .and_then(|u| u.clone().into_string())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("RPC URL missing: {}", RPC_URL_KEY),
            })
            .unwrap();

        let rpc_timeout = config
            .rpc_config
            .get(RPC_TIMEOUT_KEY)
            .and_then(|v| v.to_num())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("RPC timeout missing: {}", RPC_TIMEOUT_KEY),
            })
            .unwrap()
            .to_u32()
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("RPC timeout invalid: {}", RPC_TIMEOUT_KEY),
            })
            .unwrap();

        // Instantiate RPC client.
        let rpc_client =
            RpcClient::new_with_timeout(rpc_url, Duration::from_secs(rpc_timeout as u64));

        let big_table_creds = config
            .big_table_config
            .get(BIG_TABLE_CREDS_KEY)
            .and_then(|v| v.as_str())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("big table creds are missing: {}", BIG_TABLE_CREDS_KEY),
            })
            .unwrap();

        let big_table_timeout = config
            .rpc_config
            .get(BIG_TABLE_TIMEOUT_KEY)
            .and_then(|v| v.to_num())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("big table timeout missing: {}", BIG_TABLE_TIMEOUT_KEY),
            })
            .unwrap()
            .to_u32()
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("big table timeout invalid: {}", BIG_TABLE_TIMEOUT_KEY),
            })
            .unwrap();

        // Instantiate big table client.
        let big_table_client = LedgerStorage::new(
            true,
            Some(Duration::from_secs(big_table_timeout as u64)),
            Some(big_table_creds.to_string()),
        )
        .await
        .unwrap();

        // Instantiate messenger.
        let mut messenger = T::new(config.messenger_config).await.unwrap();
        messenger.add_stream(TRANSACTION_STREAM).await.unwrap();
        messenger.set_buffer_size(TRANSACTION_STREAM, 5000).await;

        Self {
            db,
            listener,
            rpc_client,
            big_table_client,
            messenger,
            failure_delay: INITIAL_FAILURE_DELAY,
            cache,
        }
    }

    async fn run_finder(&mut self) {
        let mut interval = time::interval(tokio::time::Duration::from_secs(TREE_SYNC_INTERVAL));
        let sem = Semaphore::new(1);
        loop {
            interval.tick().await;
            let _permit = sem.acquire().await.unwrap();
            println!("Looking for missing trees...");
            let missing = self.get_missing_trees(&self.db).await;

            match missing {
                Ok(missing_trees) => {
                    let txn = self.db.begin().await.unwrap();
                    let len = missing_trees.len();
                    statsd_gauge!("ingester.backfiller.missing_trees", len as f64);
                    println!("Found {} missing trees", len);
                    if len > 0 {
                        let res = self.force_backfill_missing_trees(missing_trees, &txn).await.err();

                        if let Some(err) = res {
                            println!("Error forcing backfill of missing trees: {}", err);
                        } else {
                            let res = txn.commit().await;
                            match res {
                                Ok(_x) => {
                                    println!("Set {} trees to backfill from 0", len);
                                }
                                Err(e) => {
                                    println!("Error setting trees to backfill from 0: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Error getting missing trees: {}", e);
                }
            }
        }
    }
    /// Run the backfiller task.
    async fn run_filler(&mut self) {
        // This is always looping, but if there are no trees to backfill, it will wait for a
        // notification on the db listener channel before continuing.
        let mut interval =
            time::interval(tokio::time::Duration::from_millis(MAX_BACKFILL_CHECK_WAIT));
        loop {
            match self.get_trees_to_backfill().await {
                Ok(backfill_trees) => {
                    if backfill_trees.is_empty() {
                        tokio::select! {
                            _ = interval.tick() => {

                            }
                            _ = self.listener.recv() => {

                            }
                        }
                    } else {
                        // First just check if we can talk to an RPC provider.
                        match self.rpc_client.get_version().await {
                            Ok(version) => println!("RPC client version {version}"),
                            Err(err) => {
                                println!("RPC client error {err}");
                                self.sleep_and_increase_delay().await;
                                continue;
                            }
                        }

                        for backfill_tree in backfill_trees {
                            for tries in 1..=NUM_TRIES {
                                // Get the tree out of nested structs.
                                let tree = &backfill_tree.unique_tree.tree;
                                let tree_string = bs58::encode(&tree).into_string();
                                println!("Backfilling tree: {tree_string}");
                                // Call different methods based on whether tree needs to be backfilled
                                // completely from seq number 1 or just have any gaps in seq number
                                // filled.
                                let result = if backfill_tree.backfill_from_seq_1 {
                                    self.backfill_tree_from_seq_1(&backfill_tree).await
                                } else {
                                    self.fetch_and_plug_gaps(tree).await
                                };

                                match result {
                                    Ok(opt_max_seq) => {
                                        // Successfully backfilled the tree.  Now clean up database.
                                        self.clean_up_backfilled_tree(
                                            opt_max_seq,
                                            tree,
                                            &tree_string,
                                            tries,
                                        )
                                        .await;
                                        self.reset_delay();
                                        break;
                                    }
                                    Err(err) => {
                                        println!("Failed to fetch and plug gaps for {tree_string}, attempt {tries}");
                                        println!("{err}");
                                    }
                                }

                                if tries == NUM_TRIES {
                                    if let Err(err) = self.mark_tree_as_failed(tree).await {
                                        println!("Error marking tree as failed to backfill: {err}");
                                    }
                                } else {
                                    self.sleep_and_increase_delay().await;
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    // Print error but keep trying.
                    println!("Could not get trees to backfill from db: {err}");
                    self.sleep_and_increase_delay().await;
                }
            }
        }
    }

    async fn force_backfill_missing_trees(
        &mut self,
        missing_trees: Vec<MissingTree>,
        cn: &impl ConnectionTrait,
    ) -> Result<(), IngesterError> {
        let trees = missing_trees
            .into_iter()
            .map(|tree| backfill_items::ActiveModel {
                tree: Set(tree.tree.as_ref().to_vec()),
                seq: Set(0),
                slot: Set(tree.slot as i64),
                force_chk: Set(true),
                backfilled: Set(false),
                failed: Set(false),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        backfill_items::Entity::insert_many(trees).exec(cn).await?;

        Ok(())
    }

    async fn clean_up_backfilled_tree(
        &mut self,
        opt_max_seq: Option<i64>,
        tree: &[u8],
        tree_string: &String,
        tries: i32,
    ) {
        match opt_max_seq {
            Some(max_seq) => {
                // Debug.
                println!("Successfully backfilled tree: {tree_string}, attempt {tries}");

                // Delete extra rows and mark as backfilled.
                match self
                    .delete_extra_rows_and_mark_as_backfilled(tree, max_seq)
                    .await
                {
                    Ok(_) => {
                        // Debug.
                        println!("Successfully deleted rows up to {max_seq}");
                    }
                    Err(err) => {
                        println!("Error deleting rows and marking as backfilled: {err}");
                        if let Err(err) = self.mark_tree_as_failed(tree).await {
                            println!("Error marking tree as failed to backfill: {err}");
                        }
                    }
                }
            }
            None => {
                // Debug.
                println!("Unexpected error, tree was in list, but no rows found for {tree_string}");
                if let Err(err) = self.mark_tree_as_failed(tree).await {
                    println!("Error marking tree as failed to backfill: {err}");
                }
            }
        }
    }

    async fn sleep_and_increase_delay(&mut self) {
        sleep(Duration::from_millis(self.failure_delay)).await;

        // Increase failure delay up to `MAX_FAILURE_DELAY_MS`.
        self.failure_delay = self.failure_delay.saturating_mul(2);
        if self.failure_delay > MAX_FAILURE_DELAY_MS {
            self.failure_delay = MAX_FAILURE_DELAY_MS;
        }
    }

    fn reset_delay(&mut self) {
        self.failure_delay = INITIAL_FAILURE_DELAY;
    }

    async fn get_missing_trees(
        &self,
        cn: &impl ConnectionTrait,
    ) -> Result<Vec<MissingTree>, IngesterError> {
        let mut all_trees: HashMap<Pubkey, u64> = self.fetch_trees_by_gpa().await?;
        println!("number of total trees: {}", all_trees.len());
        let get_locked_or_failed_trees = Statement::from_string(
            DbBackend::Postgres,
            "SELECT DISTINCT tree FROM backfill_items WHERE failed = true\n\
             OR locked = true"
                .to_string(),
        );
        let locked_trees = cn.query_all(get_locked_or_failed_trees).await?;
        for row in locked_trees.into_iter() {
            let tree = UniqueTree::from_query_result(&row, "")?;
            let key = &Pubkey::new(&tree.tree);
            if all_trees.contains_key(key) {
                all_trees.remove(key);
            }
        }
        let get_all_local_trees = Statement::from_string(
            DbBackend::Postgres,
            "SELECT DISTINCT cl_items.tree FROM cl_items".to_string(),
        );
        let force_chk_trees = cn.query_all(get_all_local_trees).await?;
        for row in force_chk_trees.into_iter() {
            let tree = UniqueTree::from_query_result(&row, "")?;
            let key = &Pubkey::new(&tree.tree);
            if all_trees.contains_key(key) {
                all_trees.remove(key);
            }
        }
        let missing_trees = all_trees
            .into_iter()
            .map(|(k, s)| MissingTree { tree: k, slot: s })
            .collect::<Vec<MissingTree>>();
        println!("number of missing trees: {}", missing_trees.len());
        Ok(missing_trees)
    }

    async fn get_trees_to_backfill(&self) -> Result<Vec<BackfillTree>, DbErr> {
        // Start a db transaction.
        let txn = self.db.begin().await?;

        // Lock the backfill_items table.
        txn.execute(Statement::from_string(
            DbBackend::Postgres,
            "LOCK TABLE backfill_items IN ACCESS EXCLUSIVE MODE".to_string(),
        ))
        .await?;

        // Get trees with the `force_chk` flag set to true (that have not failed and are not locked).
        let force_chk_trees = Statement::from_string(
            DbBackend::Postgres,
            "SELECT DISTINCT backfill_items.tree, backfill_items.slot FROM backfill_items\n\
            WHERE backfill_items.force_chk = TRUE\n\
            AND backfill_items.failed = FALSE\n\
            AND backfill_items.locked = FALSE"
                .to_string(),
        );

        let force_chk_trees: Vec<TreeWithSlot> =
            txn.query_all(force_chk_trees).await.map(|qr| {
                qr.iter()
                    .map(|q| TreeWithSlot::from_query_result(q, "").unwrap())
                    .collect()
            })?;

        println!(
            "Number of force check trees to backfill: {} {}",
            force_chk_trees.len(),
            Utc::now()
        );

        for tree in force_chk_trees.iter() {
            let stmt = backfill_items::Entity::update_many()
                .col_expr(backfill_items::Column::Locked, Expr::value(true))
                .filter(backfill_items::Column::Tree.eq(&*tree.tree))
                .build(DbBackend::Postgres);

            if let Err(err) = txn.execute(stmt).await {
                println!(
                    "Error marking tree {} as locked: {}",
                    bs58::encode(&tree.tree).into_string(),
                    err
                );
                return Err(err);
            }
        }

        // Get trees with multiple rows from `backfill_items` table (that have not failed and are not locked).
        let multi_row_trees = Statement::from_string(
            DbBackend::Postgres,
            "SELECT backfill_items.tree, max(backfill_items.slot) as slot FROM backfill_items\n\
            WHERE backfill_items.failed = FALSE
            AND backfill_items.locked = FALSE\n\
            GROUP BY backfill_items.tree\n\
            HAVING COUNT(*) > 1"
                .to_string(),
        );

        let multi_row_trees: Vec<TreeWithSlot> =
            txn.query_all(multi_row_trees).await.map(|qr| {
                qr.iter()
                    .map(|q| TreeWithSlot::from_query_result(q, "").unwrap())
                    .collect()
            })?;

        println!(
            "Number of multi-row trees to backfill {}",
            multi_row_trees.len()
        );

        for tree in multi_row_trees.iter() {
            let stmt = backfill_items::Entity::update_many()
                .col_expr(backfill_items::Column::Locked, Expr::value(true))
                .filter(backfill_items::Column::Tree.eq(&*tree.tree))
                .build(DbBackend::Postgres);

            if let Err(err) = txn.execute(stmt).await {
                println!(
                    "Error marking tree {} as locked: {}",
                    bs58::encode(&tree.tree).into_string(),
                    err
                );
                return Err(err);
            }
        }

        // Close out transaction and relinqish the lock.
        txn.commit().await?;

        // Convert force check trees Vec of `UniqueTree` to a Vec of `BackfillTree` (which contain extra info).
        let mut trees: Vec<BackfillTree> = force_chk_trees
            .into_iter()
            .map(|tree| BackfillTree::new(UniqueTree { tree: tree.tree }, true, tree.slot as u64))
            .collect();

        // Convert multi-row trees Vec of `UniqueTree` to a Vec of `BackfillTree` (which contain extra info).
        let mut multi_row_trees: Vec<BackfillTree> = multi_row_trees
            .into_iter()
            .map(|tree| BackfillTree::new(UniqueTree { tree: tree.tree }, false, tree.slot as u64))
            .collect();

        trees.append(&mut multi_row_trees);

        Ok(trees)
    }

    async fn backfill_tree_from_seq_1(
        &mut self,
        btree: &BackfillTree,
    ) -> Result<Option<i64>, IngesterError> {
        let address = Pubkey::new(btree.unique_tree.tree.as_slice());

        let slots = self.find_slots_via_address(&address).await?;

        let address = btree.unique_tree.tree.clone();
        self.plug_gap(&slots, &address).await?;

        Ok(Some(0))
    }

    async fn find_slots_via_address(&self, address: &Pubkey) -> Result<Vec<Slot>, IngesterError> {
        let mut last_sig = None;
        let mut slots = HashSet::new();
        // TODO: Any log running function like this should actually be run in a way that supports re-entry,
        // usually we woudl break the tasks into smaller parralel tasks and we woudl not worry about it, but in this we have several linearally dpendent async tasks
        // and if they fail, it causes a chain reaction of failures since the dependant nature of it affects the next task. Right now you are just naivley looping and
        // hoping for the best what needs to happen is to start saving the state opf each task with the last signature that was retuned iun durable storage.
        // Then if the task fails, you can restart it from the last signature that was returned.
        loop {
            let before = last_sig;
            let sigs = self
                .big_table_client
                .get_confirmed_signatures_for_address(address, before.as_ref(), None, 1000)
                .await
                .map_err(|e| {
                    IngesterError::RpcGetDataError(format!(
                        "GetSignaturesForAddressWithConfig failed {}",
                        e
                    ))
                })?;
            for sig in sigs.iter() {
                let slot = sig.0.slot;

                slots.insert(slot);
                last_sig = Some(sig.0.signature);
            }
            if sigs.is_empty() || sigs.len() < 1000 {
                break;
            }
        }
        Ok(Vec::from_iter(slots))
    }

    async fn get_max_seq(&self, tree: &[u8]) -> Result<Option<i64>, DbErr> {
        let query = backfill_items::Entity::find()
            .select_only()
            .column(backfill_items::Column::Seq)
            .filter(backfill_items::Column::Tree.eq(tree))
            .order_by_desc(backfill_items::Column::Seq)
            .limit(1)
            .build(DbBackend::Postgres);

        let start_seq_vec = MaxSeqItem::find_by_statement(query).all(&self.db).await?;

        Ok(start_seq_vec.last().map(|row| row.seq))
    }

    async fn clear_force_chk_flag(&self, tree: &[u8]) -> Result<UpdateResult, DbErr> {
        backfill_items::Entity::update_many()
            .col_expr(backfill_items::Column::ForceChk, Expr::value(false))
            .filter(backfill_items::Column::Tree.eq(tree))
            .exec(&self.db)
            .await
    }

    async fn fetch_trees_by_gpa(&self) -> Result<HashMap<Pubkey, u64>, IngesterError> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                0,
                vec![1u8],
            ))]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..RpcAccountInfoConfig::default()
            },
            ..RpcProgramAccountsConfig::default()
        };
        let results: Vec<(Pubkey, Account)> = self
            .rpc_client
            .get_program_accounts_with_config(&spl_account_compression::id(), config)
            .await
            .map_err(|e| IngesterError::RpcGetDataError(e.to_string()))?;
        let mut list = HashMap::with_capacity(results.len());
        for r in results.into_iter() {
            let (pubkey, account) = r;
            let mut sl = account.data.as_slice();
            let header: ConcurrentMerkleTreeHeader =
                ConcurrentMerkleTreeHeader::deserialize(&mut sl)
                    .map_err(|e| IngesterError::RpcGetDataError(e.to_string()))?;
            let auth = Pubkey::find_program_address(&[pubkey.as_ref()], &mpl_bubblegum::id()).0;
            if header.assert_valid_authority(&auth).is_err() {
                continue;
            }
            list.insert(pubkey, header.get_creation_slot());
        }
        Ok(list)
    }

    // Similar to `fetchAndPlugGaps()` in `backfiller.ts`.
    async fn fetch_and_plug_gaps(&mut self, tree: &[u8]) -> Result<Option<i64>, IngesterError> {
        let (opt_max_seq, gaps) = self.get_missing_data(tree).await?;

        self.plug_gap(&gaps, tree).await?;

        Ok(opt_max_seq)
    }

    async fn get_trees_signature(
        &self,
        tree: &[u8],
        slot: &u64,
    ) -> Result<Signature, IngesterError> {
        let block = self
            .big_table_client
            .get_confirmed_block(*slot)
            .await
            .map_err(|e| IngesterError::RpcGetDataError(e.to_string()))?;

        let cost = cmp::min(32, block.transactions.len() as i64);
        let key = format!("block{}", slot);

        let write = self
            .cache
            .try_insert_with_ttl(
                key.clone(),
                block.clone(),
                cost,
                Duration::from_secs(BLOCK_CACHE_DURATION),
            )
            .await
            .map_err(|e| IngesterError::CacheStorageWriteError(e.to_string()))?;

        if !write {
            return Err(IngesterError::CacheStorageWriteError(
                "Failed to write to cache. Method - get_trees_signature()".to_string(),
            ));
        }

        self.cache.wait().await.unwrap();

        // get transactions
        for tx in block.transactions.iter() {
            let meta = if let Some(meta) = tx.get_status_meta() {
                if let Err(err) = meta.status {
                    continue;
                }
                meta
            } else {
                continue;
            };

            let decoded_tx = tx.get_transaction();

            let msg = decoded_tx.message;

            let atl_keys = msg.address_table_lookups();

            let account_keys = msg.static_account_keys();
            let account_keys = {
                let mut account_keys_vec = vec![];
                for key in account_keys.iter() {
                    account_keys_vec.push(key.to_bytes());
                }
                if atl_keys.is_some() {
                    let ad = meta.loaded_addresses;

                    for i in &ad.writable {
                        account_keys_vec.push(i.to_bytes());
                    }

                    for i in &ad.readonly {
                        account_keys_vec.push(i.to_bytes());
                    }
                }
                account_keys_vec
            };

            let bubblegum = blockbuster::programs::bubblegum::program_id().to_bytes();
            if account_keys
                .iter()
                .all(|pk| *pk == tree || *pk == bubblegum)
            {
                return Ok(decoded_tx.signatures[0]);
            }
        }

        let tree_pubkey = Pubkey::try_from_slice(tree).unwrap();

        Err(IngesterError::SlotDoesntHaveTreeSignatures(
            tree_pubkey.to_string(),
        ))
    }

    // Similar to `getMissingData()` in `db.ts`.
    async fn get_missing_data(
        &self,
        tree: &[u8],
    ) -> Result<(Option<i64>, Vec<u64>), IngesterError> {
        // Get the maximum sequence number that has been backfilled, and use
        // that for the starting sequence number for backfilling.
        let query = backfill_items::Entity::find()
            .select_only()
            .column(backfill_items::Column::Seq)
            .filter(backfill_items::Column::Tree.eq(tree))
            .filter(backfill_items::Column::Backfilled.eq(true))
            .order_by_desc(backfill_items::Column::Seq)
            .limit(1)
            .build(DbBackend::Postgres);

        let start_seq_vec = MaxSeqItem::find_by_statement(query).all(&self.db).await?;
        let start_seq = if let Some(seq) = start_seq_vec.last().map(|row| row.seq) {
            seq
        } else {
            0
        };

        // Get all rows for the tree that have not yet been backfilled.
        let mut query = backfill_items::Entity::find()
            .select_only()
            .column(backfill_items::Column::Seq)
            .column(backfill_items::Column::Slot)
            .filter(backfill_items::Column::Seq.gte(start_seq))
            .filter(backfill_items::Column::Tree.eq(tree))
            .order_by_asc(backfill_items::Column::Seq)
            .build(DbBackend::Postgres);

        query.sql = query.sql.replace("SELECT", "SELECT DISTINCT");
        let rows = SimpleBackfillItem::find_by_statement(query)
            .all(&self.db)
            .await?;
        let mut gaps = vec![];

        // Look at each pair of subsequent rows, looking for a gap in sequence number.
        for (prev, curr) in rows.iter().zip(rows.iter().skip(1)) {
            if curr.seq == prev.seq {
                let message = format!(
                    "Error in DB, identical sequence numbers with different slots: {}, {}",
                    prev.slot, curr.slot
                );
                println!("{}", message);
                return Err(IngesterError::DbError(message));
            } else if curr.seq - prev.seq > 1 {
                gaps.push((prev.slot as u64, curr.slot as u64));
            }
        }

        let mut signatures = Vec::new();

        for (start, end) in gaps.iter() {
            let signature_from = self.get_trees_signature(tree, start).await?;

            let signature_to = self.get_trees_signature(tree, end).await?;

            signatures.push((signature_from, signature_to));
        }

        let mut slots = Vec::new();

        let tree_pubkey = Pubkey::try_from_slice(tree).unwrap();

        for (sig_start, sig_end) in signatures.iter() {
            loop {
                let sigs = self
                    .big_table_client
                    .get_confirmed_signatures_for_address(
                        &tree_pubkey,
                        Some(sig_end),
                        Some(sig_start),
                        1000,
                    )
                    .await
                    .map_err(|e| {
                        IngesterError::BigTableError(format!(
                            "GetSignaturesForAddress failed {}",
                            e
                        ))
                    })?;

                for sig in sigs.iter() {
                    let slot = sig.0.slot;
                    slots.push(slot);
                }

                if sigs.is_empty() || sigs.len() < 1000 {
                    break;
                }
            }
        }

        // Get the max sequence number if any rows were returned from the query.
        let opt_max_seq = rows.last().map(|row| row.seq);

        Ok((opt_max_seq, slots))
    }

    async fn plug_gap(&mut self, slots: &Vec<u64>, tree: &[u8]) -> Result<(), IngesterError> {
        for slot in slots {
            let key = format!("block{}", slot);
            let mut cached_block = self.cache.get(&key);
            if cached_block.is_none() {
                println!("Fetching block {} from RPC", slot);
                let block = self
                    .big_table_client
                    .get_confirmed_block(*slot)
                    .await
                    .map_err(|e| IngesterError::BigTableError(e.to_string()))?;

                let cost = cmp::min(32, block.transactions.len() as i64);

                let write = self
                    .cache
                    .try_insert_with_ttl(
                        key.clone(),
                        block,
                        cost,
                        Duration::from_secs(BLOCK_CACHE_DURATION),
                    )
                    .await?;

                if !write {
                    return Err(IngesterError::CacheStorageWriteError(format!(
                        "Cache Write Failed on {} is missing.",
                        &key
                    )));
                }

                self.cache.wait().await?;
                cached_block = self.cache.get(&key);
            }
            if cached_block.is_none() {
                return Err(IngesterError::CacheStorageWriteError(format!(
                    "Cache Procedure Failed {} is missing.",
                    &key
                )));
            }
            let block_ref = cached_block.unwrap();
            let block_data = block_ref.value();

            for tx in block_data.transactions.iter() {
                let meta = if let Some(meta) = tx.get_status_meta() {
                    if let Err(_err) = meta.status {
                        continue;
                    }
                    meta
                } else {
                    println!("Unexpected, EncodedTransactionWithStatusMeta struct has no metadata");
                    continue;
                };

                let decoded_tx = tx.get_transaction();

                let sig = decoded_tx.signatures[0].to_string();

                let msg = decoded_tx.message;
                let atl_keys = msg.address_table_lookups();

                let tree = Pubkey::try_from(tree)
                    .map_err(|e| IngesterError::DeserializationError(e.to_string()))?;

                let account_keys = msg.static_account_keys();

                let account_keys = {
                    let mut account_keys_vec = vec![];

                    for key in account_keys.iter() {
                        account_keys_vec.push(key.to_bytes());
                    }
                    if atl_keys.is_some() {
                        let ad = meta.loaded_addresses;

                        for i in &ad.writable {
                            account_keys_vec.push(i.to_bytes());
                        }

                        for i in &ad.readonly {
                            account_keys_vec.push(i.to_bytes());
                        }
                        // }
                    }
                    account_keys_vec
                };

                // Filter out transactions that don't have to do with the tree we are interested in or
                // the Bubblegum program.
                let tb = tree.to_bytes();
                let bubblegum = blockbuster::programs::bubblegum::program_id().to_bytes();
                if account_keys.iter().all(|pk| *pk != tb && *pk != bubblegum) {
                    continue;
                }

                // Serialize data.
                let builder = FlatBufferBuilder::new();
                println!("Serializing transaction in backfiller {}", sig);

                // tx version 0 to support transactions with address lookup table
                let encoded_tx = tx
                    .clone()
                    .encode(UiTransactionEncoding::Base58, Some(0), false)
                    .unwrap();

                let tx_wrap = EncodedConfirmedTransactionWithStatusMeta {
                    transaction: encoded_tx,
                    slot: *slot,
                    block_time: block_data.block_time,
                };
                let builder = seralize_encoded_transaction_with_status(builder, tx_wrap)?;
                self.messenger
                    .send(TRANSACTION_STREAM, builder.finished_data())
                    .await?;
            }
            drop(block_ref);
        }

        Ok(())
    }

    async fn delete_extra_rows_and_mark_as_backfilled(
        &self,
        tree: &[u8],
        max_seq: i64,
    ) -> Result<(), DbErr> {
        // Debug.
        let test_items = backfill_items::Entity::find()
            .filter(backfill_items::Column::Tree.eq(tree))
            .all(&self.db)
            .await?;
        println!("Count of items before delete: {}", test_items.len());
        for item in test_items {
            println!("Seq ID {}", item.seq);
        }

        // Delete all rows in the `backfill_items` table for a specified tree, except for the row with
        // the caller-specified max seq number.  One row for each tree must remain so that gaps can be
        // detected after subsequent inserts.
        backfill_items::Entity::delete_many()
            .filter(backfill_items::Column::Tree.eq(tree))
            .filter(backfill_items::Column::Seq.ne(max_seq))
            .exec(&self.db)
            .await?;

        // Remove any duplicates that have the caller-specified max seq number.  This happens when
        // a transaction that was already handled is replayed during backfilling.
        let items = backfill_items::Entity::find()
            .filter(backfill_items::Column::Tree.eq(tree))
            .filter(backfill_items::Column::Seq.eq(max_seq))
            .all(&self.db)
            .await?;

        if items.len() > 1 {
            for item in items.iter().skip(1) {
                backfill_items::Entity::delete_by_id(item.id)
                    .exec(&self.db)
                    .await?;
            }
        }

        // Mark remaining row as backfilled so future backfilling can start above this sequence number.
        self.mark_tree_as_backfilled(tree).await?;

        // Clear the `force_chk` flag if it was set.
        self.clear_force_chk_flag(tree).await?;

        // Unlock tree.
        self.unlock_tree(tree).await?;

        // Debug.
        let test_items = backfill_items::Entity::find()
            .filter(backfill_items::Column::Tree.eq(tree))
            .all(&self.db)
            .await?;
        println!("Count of items after delete: {}", test_items.len());
        for item in test_items {
            println!("Seq ID {}", item.seq);
        }

        Ok(())
    }

    async fn mark_tree_as_backfilled(&self, tree: &[u8]) -> Result<(), DbErr> {
        backfill_items::Entity::update_many()
            .col_expr(backfill_items::Column::Backfilled, Expr::value(true))
            .filter(backfill_items::Column::Tree.eq(tree))
            .exec(&self.db)
            .await?;

        Ok(())
    }

    async fn mark_tree_as_failed(&self, tree: &[u8]) -> Result<(), DbErr> {
        backfill_items::Entity::update_many()
            .col_expr(backfill_items::Column::Failed, Expr::value(true))
            .filter(backfill_items::Column::Tree.eq(tree))
            .exec(&self.db)
            .await?;

        Ok(())
    }

    async fn unlock_tree(&self, tree: &[u8]) -> Result<(), DbErr> {
        backfill_items::Entity::update_many()
            .col_expr(backfill_items::Column::Locked, Expr::value(false))
            .filter(backfill_items::Column::Tree.eq(tree))
            .exec(&self.db)
            .await?;

        Ok(())
    }
}
