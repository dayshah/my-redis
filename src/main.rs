use std::collections::HashMap;
use mini_redis::Command::*;
use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { process(socket) });
    }
}

async fn process(socket: TcpStream) {
    let mut db = HashMap::new();
    let mut connection = Connection::new(socket);
    if let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match mini_redis::Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_owned(), cmd.value().to_owned());
                Frame::Simple("Ok".to_owned())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("still gotta get to it {:?}", cmd)
        };
        connection.write_frame(&response).await.unwrap();
    }
}
