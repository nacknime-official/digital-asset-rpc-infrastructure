use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use solana_geyser_zmq::receiver::TcpReceiver;

pub struct RoutingTcpReceiver {
    inner: TcpReceiver,
    callbacks: Arc<Mutex<HashMap<u8, Box<dyn Fn(Vec<u8>) + Send>>>>,
}

impl RoutingTcpReceiver {
    pub fn new(connect_timeout: Duration, reconnect_interval: Duration) -> Self {
        let callbacks: Arc<Mutex<HashMap<u8, Box<dyn Fn(Vec<u8>) + Send>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let callbacks_clone = Arc::clone(&callbacks);

        RoutingTcpReceiver {
            inner: TcpReceiver::new(
                Box::new(move |data| {
                    let msg_type = data[0];
                    if let Some(callback) = callbacks_clone.lock().unwrap().get(&msg_type) {
                        callback(data[1..].to_vec());
                    }
                }),
                connect_timeout,
                reconnect_interval,
            ),
            callbacks,
        }
    }

    pub fn register_callback(&self, msg_type: u8, callback: Box<dyn Fn(Vec<u8>) + Send>) {
        self.callbacks.lock().unwrap().insert(msg_type, callback);
    }

    pub fn connect(&self, addr: SocketAddr) -> std::io::Result<()> {
        self.inner.connect(addr)
    }
}
