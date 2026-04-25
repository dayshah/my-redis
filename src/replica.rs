use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::net::TcpStream;

use crate::commands::apply_set;
use crate::options::Options;
use crate::resp::{self, Message};
use crate::state::ServerState;
use crate::stream_reader::StreamReaderWriter;

pub async fn run(options: &Options, state: &ServerState) {
    let replica_options = options.replica_options.as_ref().unwrap();
    let master_host = &replica_options.master_host;
    let master_port = &replica_options.master_port;
    let stream = TcpStream::connect(format!("{}:{}", master_host, master_port))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Couldn't connect to master at {}:{}",
                master_host, master_port
            )
        });
    let mut streamer = StreamReaderWriter::new(stream);

    // PING
    streamer.write(b"*1\r\n$4\r\nPING\r\n").await;
    let (msg, _) = streamer.get_message().await.unwrap();
    assert_eq!(msg[0].as_ref().unwrap(), "PONG");

    // REPLCONF listening-port <port>
    let replconf_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
        options.port
    );
    streamer.write(replconf_port.as_bytes()).await;
    let (msg, _) = streamer.get_message().await.unwrap();
    assert_eq!(msg[0].as_ref().unwrap(), "OK");

    // REPLCONF capa psync2
    streamer
        .write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await;
    let (msg, _) = streamer.get_message().await.unwrap();
    assert_eq!(msg[0].as_ref().unwrap(), "OK");

    // PSYNC ? -1
    streamer
        .write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await;
    let (msg, _) = streamer.get_message().await.unwrap();
    assert_eq!(&msg[0].as_ref().unwrap()[..10], "FULLRESYNC");
    streamer.read_rdb_file().await;

    println!("Replica starting to accept writes from master");
    let cache = state.cache.clone();
    tokio::spawn(run_follower(streamer, cache));
}

async fn run_follower(
    mut streamer: StreamReaderWriter,
    cache: Arc<Mutex<HashMap<String, String>>>,
) {
    let mut track_read_bytes = false;
    let mut tracked_read_bytes: usize = 0;
    while let Some((mut message_vec, bytes_read)) = streamer.get_message().await {
        let command = message_vec[0].as_ref().unwrap().to_ascii_lowercase();
        match command.as_ref() {
            "set" => {
                let key = message_vec[1].take().unwrap();
                let value = message_vec[2].take().unwrap();
                apply_set(&cache, key, value, &message_vec);
            }
            "replconf" => {
                track_read_bytes = true;
                let response = resp::convert_message(Message::Array(vec![
                    Message::BulkStatic("REPLCONF"),
                    Message::BulkStatic("ACK"),
                    Message::Bulk(tracked_read_bytes.to_string()),
                ]));
                streamer.write(response.as_bytes()).await;
            }
            _ => println!("Unexpected command sent to replica: {}", command),
        }
        if track_read_bytes {
            tracked_read_bytes += bytes_read;
        }
    }
}
