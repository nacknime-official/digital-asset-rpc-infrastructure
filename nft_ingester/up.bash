export RUST_LOG=debug
export INGESTER_DATABASE_CONFIG='{max_postgres_connections=, listener_channel="backfill_item_added", url="postgres://ingester:ingester@localhost/ingester"}'
export INGESTER_MESSENGER_CONFIG='{connection_config={batch_size=500,idle_timeout=5000,redis_connection_str="redis://redis"}, messenger_type="Redis"}'
export INGESTER_RPC_CONFIG='{url="http://${COMPOSE_PROJECT_NAME}-solana-1:8899/", commitment="confirmed"}'
export RUST_BACKTRACE=1
cargo run
