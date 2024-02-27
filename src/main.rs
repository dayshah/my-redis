use std::{collections::HashMap, sync::{Arc, Mutex}};
use tokio::net::{TcpListener, TcpStream};
use bytes::Bytes;

type DB = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: DB = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db_clone = db.clone();
        tokio::spawn(async move { process(socket, db_clone) });
    }
}

async fn process(socket: TcpStream, db: DB) {
    use mini_redis::{Connection, Frame, Command::*};
    let mut connection = Connection::new(socket);
    if let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match mini_redis::Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_owned(), cmd.value().clone());
                Frame::Simple("Ok".to_owned())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("still gotta get to it {:?}", cmd)
        };
        connection.write_frame(&response).await.unwrap();
    }
}
