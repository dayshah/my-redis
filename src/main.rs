use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;

mod commands;
mod options;
mod replica;
mod replicas;
mod resp;
mod state;
mod stream_reader;

use commands::{ConnectionRole, accept_command};
use replicas::{Replicas, run_forwarder};
use state::{ClientState, MasterData, ServerState};
use stream_reader::StreamReaderWriter;

use crate::resp::Message;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    println!("Starting my redis!");
    let options = options::parse_args();

    let master_data = Arc::new(options.replica_options.is_none().then(|| MasterData {
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        master_repl_offset: AtomicU64::new(0),
    }));

    let state = ServerState {
        cache: Arc::new(Mutex::new(HashMap::new())),
        master_data,
        replicas: Arc::new(Replicas::new()),
        pending_writes: Arc::new(AtomicBool::new(false)),
        watched_keys: Arc::new(Mutex::new(HashMap::new())),
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port))
        .await
        .unwrap_or_else(|_| panic!("Couldn't bind to port {}", options.port));

    // Replica will first sync with leader, return, and then spawn another task that will
    // just keep receiving messages from leader.
    if options.replica_options.is_some() {
        replica::run(&options, &state).await;
    }

    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        let streamer = StreamReaderWriter::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            println!("Accepted new connection from {}", addr);
            handle_connection(streamer, state).await;
        });
    }
}

const MULTI_COMMANDS: [&str; 3] = ["exec", "discard", "watch"];

async fn handle_connection(mut streamer: StreamReaderWriter, state: ServerState) {
    let mut client_state = ClientState {
        multi_queue: None,
        currently_watching: HashSet::new(),
    };
    while let Some((message_vec, _)) = streamer.get_message().await {
        let command = message_vec[0].as_ref().unwrap().to_ascii_lowercase();

        if let Some(multi_queue) = &mut client_state.multi_queue
            && !MULTI_COMMANDS.contains(&command.as_str())
        {
            multi_queue.push_back(message_vec);
            streamer
                .write(resp::convert_message(Message::SimpleStatic("QUEUED")).as_bytes())
                .await;
            continue;
        }
        let (role, result_message) =
            accept_command(message_vec, &mut streamer, &state, &mut client_state).await;
        streamer
            .write(resp::convert_message(result_message).as_bytes())
            .await;
        if let ConnectionRole::BecameReplica = role {
            run_forwarder(streamer, &state.replicas).await;
            return;
        }
    }
}
