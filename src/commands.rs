use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::resp::{self, Message};
use crate::state::{ClientState, MasterData, ServerState};
use crate::stream_reader::StreamReaderWriter;

pub enum ConnectionRole {
    Client,
    BecameReplica,
}

pub async fn accept_command(
    mut message_vec: Vec<Option<String>>,
    streamer: &mut StreamReaderWriter,
    state: &ServerState,
    client_state: &mut ClientState,
) -> (ConnectionRole, Message) {
    let command = message_vec[0].as_ref().unwrap().to_ascii_lowercase();
    let mut role = ConnectionRole::Client;

    let result_message = match command.as_ref() {
        "echo" => {
            let echo_message = message_vec[1].take().unwrap();
            Message::Bulk(echo_message)
        }
        "set" => {
            handle_set(state, message_vec).await;
            Message::SimpleStatic("OK")
        }
        "get" => {
            let cache = state.cache.lock().unwrap();
            let key = message_vec[1].as_deref().unwrap();
            if let Some(value) = cache.get(key) {
                Message::Bulk(value.clone())
            } else {
                Message::NullBulk
            }
        }
        "ping" => Message::SimpleStatic("PONG"),
        "info" => {
            let mut option = message_vec[1].take().unwrap();
            option.make_ascii_lowercase();
            assert_eq!(option, "replication");
            let result = if let Some(md) = state.master_data.as_ref() {
                format!(
                    concat!(
                        "role:master\r\n",
                        "master_replid:{}\r\n",
                        "master_repl_offset:{}\r\n"
                    ),
                    md.master_replid,
                    md.master_repl_offset.load(Ordering::Relaxed),
                )
            } else {
                "role:slave\r\n".to_string()
            };
            Message::Bulk(result)
        }
        "replconf" => Message::SimpleStatic("OK"),
        "psync" => {
            // The RDB bytes are written from run_forwarder *after* the replica is
            // registered, so a SET that races the handshake can't be lost.
            role = ConnectionRole::BecameReplica;
            let MasterData {
                master_replid,
                master_repl_offset,
            } = state.master_data.as_ref().as_ref().unwrap();
            Message::Simple(format!(
                "FULLRESYNC {} {}",
                master_replid,
                master_repl_offset.load(Ordering::Relaxed),
            ))
        }
        "wait" => {
            let num_needed = message_vec[1].take().unwrap().parse::<i64>().unwrap();
            let timeout_ms = message_vec[2].take().unwrap().parse::<u64>().unwrap();

            let num_acked = handle_wait(state, num_needed, timeout_ms).await;
            Message::Integer(num_acked)
        }
        "incr" => {
            let key = message_vec[1].take().unwrap();
            let amount = message_vec
                .get(2)
                .unwrap_or(&Some("1".to_owned()))
                .as_ref()
                .unwrap()
                .parse::<i64>()
                .unwrap();

            let mut cache = state.cache.lock().unwrap();
            let current_value = cache.get(&key).unwrap_or(&"0".to_owned()).parse::<i64>();
            if let Ok(current_value) = current_value {
                let inc_value = current_value + amount;
                cache.insert(key, inc_value.to_string());
                Message::Integer(inc_value)
            } else {
                Message::Err("value is not an integer or out of range")
            }
        }
        "multi" => {
            client_state.multi_queue = Some(VecDeque::new());
            Message::SimpleStatic("OK")
        }
        "exec" => {
            let result = handle_exec(streamer, state, client_state).await;
            client_state.multi_queue = None;
            result
        }
        "discard" => {
            if client_state.multi_queue.is_none() {
                Message::Err("DISCARD without MULTI")
            } else {
                client_state.multi_queue = None;
                Message::SimpleStatic("OK")
            }
        }
        "watch" => {
            if client_state.multi_queue.is_some() {
                Message::Err("WATCH inside MULTI is not allowed")
            } else {
                for key in message_vec.drain(1..) {
                    let key = key.unwrap();
                    let mut watched_keys = state.watched_keys.lock().unwrap();
                    watched_keys
                        .entry(key.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                    client_state.currently_watching.insert(key);
                }
                Message::SimpleStatic("OK")
            }
        }
        _ => Message::SimpleStatic("unexpected command"),
    };

    (role, result_message)
}

async fn handle_set(state: &ServerState, mut message_vec: Vec<Option<String>>) {
    let mut wire = Vec::new();
    for message in message_vec.iter() {
        wire.push(Message::Bulk(message.clone().unwrap()));
    }
    let wire = resp::convert_message(Message::Array(wire)).into_bytes();

    let key = message_vec[1].take().unwrap();
    let value = message_vec[2].take().unwrap();
    // Check if key is being watched and invalidate watchers if so
    state.watched_keys.lock().unwrap().remove(&key);

    apply_set(&state.cache, key, value, &message_vec);

    state.replicas.broadcast(wire).await;
    // Mark that a write has happened since the last WAIT. A concurrent WAIT
    // that calls swap() before this store simply sees false and no-ops —
    // that's fine because its GETACK would have been queued behind the SET
    // broadcast above, so the replica would process both in order.
    state.pending_writes.store(true, Ordering::SeqCst);
}

pub fn apply_set(
    cache: &Arc<Mutex<HashMap<String, String>>>,
    key: String,
    value: String,
    message_vec: &Vec<Option<String>>,
) {
    if let Some(option) = message_vec.get(3) {
        assert_eq!(option.as_deref().unwrap(), "PX");
        let expiry_ms = message_vec[4].as_deref().unwrap().parse::<u64>().unwrap();
        cache.lock().unwrap().insert(key.clone(), value);
        let cache = cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(expiry_ms)).await;
            cache.lock().unwrap().remove(&key);
        });
    } else {
        cache.lock().unwrap().insert(key, value);
    }
}

async fn handle_wait(state: &ServerState, num_needed: i64, timeout_ms: u64) -> i64 {
    let mut num_acked = 0;
    if !state.pending_writes.swap(false, Ordering::SeqCst) {
        // No writes since the last WAIT — every connected replica is
        // trivially caught up, so skip the round trip.
        num_acked = state.replicas.count() as i64;
    } else {
        let mut join_set = tokio::task::JoinSet::new();
        for receiver in state.replicas.request_acks().await {
            // .ok() collapses RecvError (sender dropped mid-flight) to None.
            join_set.spawn(async move { receiver.await.ok() });
        }

        let collect = async {
            while num_acked < num_needed {
                match join_set.join_next().await.unwrap() {
                    Ok(Some(_)) => num_acked += 1,
                    _ => {}
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(timeout_ms), collect).await;
    };
    num_acked
}

async fn handle_exec(
    streamer: &mut StreamReaderWriter,
    state: &ServerState,
    client_state: &mut ClientState,
) -> Message {
    // Execute the queued commands
    if client_state.multi_queue.is_none() {
        return Message::Err("EXEC without MULTI");
    } else {
        {
            let mut watched_keys = state.watched_keys.lock().unwrap();
            for key in client_state.currently_watching.drain() {
                if let Some(count) = watched_keys.get_mut(&key) {
                    *count -= 1;
                    if *count == 0 {
                        watched_keys.remove(&key);
                    }
                } else {
                    return Message::NullArray;
                }
            }
        }
        let mut result = Vec::new();
        while let Some(message_vec) = client_state.multi_queue.as_mut().unwrap().pop_front() {
            // Have to pin bc it's an unknown size bc of the recursive
            // async call so has to be on heap, and async future can't
            // move while being polled.
            let (_role, result_message) =
                Box::pin(accept_command(message_vec, streamer, state, client_state)).await;
            result.push(result_message);
        }
        return Message::Array(result);
    }
}
