export RUST_LOG=debug
export INGESTER_DATABASE_CONFIG='{max_postgres_connections=, listener_channel="backfill_item_added", url="postgres://ingester:ingester@localhost/ingester"}'
export INGESTER_MESSENGER_CONFIG='{connection_config={batch_size=500,idle_timeout=5000,redis_connection_str="redis://redis"}, messenger_type="Redis"}'
export INGESTER_RPC_CONFIG='{url="http://127.0.0.1:8899/", commitment="confirmed"}'
export INGESTER_TCP_CONFIG='{receiver_addr="127.0.0.1:3333", receiver_connect_timeout=10, receiver_reconnect_interval=5, backfiller_receiver_addr="127.0.0.1:3334", backfiller_receiver_connect_timeout=10, backfiller_receiver_reconnect_interval=5, backfiller_sender_port=3334, backfiller_sender_batch_max_bytes=1, backfiller_sender_buffer_size=1}'
export RUST_BACKTRACE=1
cargo run
