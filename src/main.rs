use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

mod options;
mod resp;
mod set;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Starting my redis!");
    let options = Arc::new(options::parse_args());
    let is_master = options.replica_options.is_none();
    let master_data = Arc::new(if is_master {
        Some(MasterData {
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        })
    } else {
        None
    });

    // TODO: Should probably start listening before sending messages to master
    if let Some(ref replica_options) = options.replica_options {
        let master_host = &replica_options.master_host;
        let master_port = &replica_options.master_port;
        let mut stream = TcpStream::connect(format!("{}:{}", master_host, master_port))
            .await
            .expect(
                format!(
                    "Couldn't connect to master at {}:{}",
                    master_host, master_port
                )
                .as_str(),
            );
        let mut buf = [0; 1024];

        // PING PONG
        stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        stream.read(&mut buf).await.unwrap();
        assert_eq!(b"+PONG\r\n", &buf[..7]);

        // REPLCONF listening-port
        let replconf = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n";
        let replconf_port = format!("{}$4\r\n{}\r\n", replconf, options.port);
        stream.write_all(replconf_port.as_bytes()).await.unwrap();
        stream.read(&mut buf).await.unwrap();
        assert_eq!(b"+OK\r\n", &buf[..5]);

        // REPLCONF capa
        let replconf_capa = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        stream.write_all(replconf_capa).await.unwrap();
        stream.read(&mut buf).await.unwrap();
        assert_eq!(b"+OK\r\n", &buf[..5]);

        //PSYNC
        let psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        stream.write_all(psync).await.unwrap();
        stream.read(&mut buf).await.unwrap();
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port))
        .await
        .expect(format!("Couldn't bind to the port {}", options.port).as_str());

    let cache = Arc::new(Mutex::new(HashMap::new()));
    let client_senders: Arc<Mutex<Vec<mpsc::Sender<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
    loop {
        let (mut stream, addr) = listener.accept().await.unwrap();
        let cache = cache.clone();
        let master_data = master_data.clone();
        let client_senders = client_senders.clone();
        tokio::spawn(async move {
            println!("accepted new connection from {}", addr);
            let mut buf = [0; 1024];
            let mut is_done_and_is_replica = false;
            while !(stream.read(&mut buf).await.unwrap_or(0) == 0) {
                is_done_and_is_replica = accept_command(
                    &mut stream,
                    &buf,
                    &cache,
                    &master_data,
                    client_senders.clone(),
                )
                .await
                .expect("Failed to accept command");
                if is_done_and_is_replica {
                    break;
                }
            }
            // Use this stream to send data to replicas
            if is_done_and_is_replica {
                let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(100);
                client_senders.lock().unwrap().push(sender);
                while let Some(message) = receiver.recv().await {
                    stream.write_all(&message).await.unwrap();
                }
            }
        });
    }
}

struct MasterData {
    master_replid: String,
    master_repl_offset: u64,
}

async fn accept_command(
    stream: &mut TcpStream,
    buf: &[u8],
    cache: &Arc<Mutex<HashMap<String, String>>>,
    master_data: &Option<MasterData>,
    client_senders: Arc<Mutex<Vec<mpsc::Sender<Vec<u8>>>>>,
) -> Option<bool> {
    use resp::Message;

    let (mut message_vec, size) = resp::parse_message(&buf).unwrap();
    let mut command = message_vec.get_mut(0)?.take()?;
    let mut is_done_and_is_replica = false;
    command.make_ascii_lowercase();

    let result_message = match command.as_str() {
        "echo" => {
            let echo_message = message_vec.get_mut(1)?.take()?;
            Some(Message::Bulk(echo_message))
        }
        "set" => Some(set::handle_set(cache, &buf, size, message_vec, client_senders).await?),
        "get" => {
            let cache = cache.lock().unwrap();
            let key = message_vec.get(1)?.as_deref()?;
            if let Some(value) = cache.get(key) {
                Some(Message::Bulk(value.clone()))
            } else {
                Some(Message::NullBulk())
            }
        }
        "ping" => Some(Message::SimpleStatic("PONG")),
        "info" => {
            let mut option = message_vec.get_mut(1)?.take()?;
            option.make_ascii_lowercase();
            assert_eq!(option, "replication");
            let result = if let Some(master_data) = master_data {
                format!(
                    concat!(
                        "role:master\r\n",
                        "master_replid:{}\r\n",
                        "master_repl_offset:{}\r\n"
                    ),
                    master_data.master_replid, master_data.master_repl_offset
                )
            } else {
                "role:slave\r\n".to_string()
            };
            Some(Message::Bulk(result))
        }
        "replconf" => Some(Message::SimpleStatic("OK")),
        "psync" => {
            let MasterData {
                master_replid,
                master_repl_offset,
            } = master_data.as_ref().unwrap();
            let resync_reply = Message::Simple(format!(
                "FULLRESYNC {} {}",
                master_replid, master_repl_offset
            ));
            let empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            let empty_file_message = parse_hex_rdb_file(empty_rdb_file_hex);

            stream
                .write_all(resp::convert_message(resync_reply).as_bytes())
                .await
                .expect("Failed to write response");
            stream
                .write_all(&empty_file_message)
                .await
                .expect("Failed to write response");

            is_done_and_is_replica = true;
            None
        }
        _ => Some(Message::SimpleStatic("unexpected command")),
    };

    if let Some(result_message) = result_message {
        stream
            .write_all(resp::convert_message(result_message).as_bytes())
            .await
            .expect("Failed to write response");
    }

    return Some(is_done_and_is_replica);
}

fn parse_hex_rdb_file(file_str: &str) -> Vec<u8> {
    let hex_len = file_str.len() / 2;
    let mut res = format!("${}\r\n", hex_len).into_bytes();
    for i in (0..file_str.len()).step_by(2) {
        res.push(u8::from_str_radix(&file_str[i..i + 2], 16).unwrap());
    }
    return res;
}
