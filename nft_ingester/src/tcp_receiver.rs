use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use log::debug;
use solana_geyser_zmq::receiver::TcpReceiver;

pub struct RoutingTcpReceiver {
    inner: TcpReceiver,
    callbacks: Arc<RwLock<HashMap<u8, Box<dyn Fn(Vec<u8>) + Send + Sync>>>>,
}

impl RoutingTcpReceiver {
    pub fn new(connect_timeout: Duration, reconnect_interval: Duration) -> Self {
        let callbacks: Arc<RwLock<HashMap<u8, Box<dyn Fn(Vec<u8>) + Send + Sync>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let callbacks_clone = Arc::clone(&callbacks);

        RoutingTcpReceiver {
            inner: TcpReceiver::new(
                Box::new(move |data| {
                    let msg_type = data[0];
                    debug!("msg_type: {:?}", msg_type);
                    let callbacks = callbacks_clone.read().unwrap();
                    if let Some(callback) = callbacks.get(&msg_type) {
                        callback(data[1..].to_vec());
                    }
                }),
                connect_timeout,
                reconnect_interval,
            ),
            callbacks,
        }
    }

    pub fn register_callback(&self, msg_type: u8, callback: Box<dyn Fn(Vec<u8>) + Send + Sync>) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks.insert(msg_type, callback);
    }

    pub fn connect(&self, addr: SocketAddr) -> std::io::Result<()> {
        self.inner.connect(addr)
    }
}
