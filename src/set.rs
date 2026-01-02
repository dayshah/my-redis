use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::resp::Message;

pub async fn handle_set(
    cache: &Arc<Mutex<HashMap<String, String>>>,
    buf: &[u8],
    size: usize,
    mut message_vec: Vec<Option<String>>,
    client_senders: Arc<Mutex<Vec<mpsc::Sender<Vec<u8>>>>>,
) -> Option<Message> {
    let key = message_vec.get_mut(1)?.take()?;
    let value = message_vec.get_mut(2)?.take()?;

    let client_senders = { client_senders.lock().unwrap().clone() };
    let mut send_futures = Vec::new();
    for sender in client_senders.iter() {
        send_futures.push(sender.send(buf[..size].to_vec()));
    }

    if let Some(option) = message_vec.get(3) {
        assert_eq!(option.as_deref()?, "PX");
        // Delete after expiry milliseconds
        let expiry_ms = message_vec.get(4)?.as_deref()?.parse::<u64>().ok()?;
        let mut unlocked_cache = cache.lock().unwrap();
        // Have to copy key in the expiry case
        unlocked_cache.insert(key.clone(), value);
        let cache = cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(expiry_ms)).await;
            let mut unlocked_cache = cache.lock().unwrap();
            unlocked_cache.remove(&key);
        });
    } else {
        let mut unlocked_cache = cache.lock().unwrap();
        unlocked_cache.insert(key, value);
    }

    for f in send_futures {
        f.await.unwrap();
    }

    return Some(Message::SimpleStatic("OK"));
}
