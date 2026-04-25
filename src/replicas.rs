use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::{mpsc, oneshot};

use crate::{
    resp::{self, Message},
    stream_reader::{self, StreamReaderWriter},
};

pub type ReplyBytes = Vec<Option<String>>;
pub type ReplicaSender = mpsc::Sender<(Vec<u8>, Option<oneshot::Sender<ReplyBytes>>)>;

pub struct Replicas {
    senders: Mutex<Vec<ReplicaSender>>,
}

impl Replicas {
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self, sender: ReplicaSender) {
        self.senders.lock().unwrap().push(sender);
    }

    pub fn count(&self) -> usize {
        self.senders.lock().unwrap().len()
    }

    pub async fn broadcast(&self, bytes: Vec<u8>) {
        let senders = { self.senders.lock().unwrap().clone() };
        for sender in senders {
            let _ = sender.send((bytes.clone(), None)).await;
        }
    }

    pub async fn request_acks(&self) -> Vec<oneshot::Receiver<ReplyBytes>> {
        let getack = resp::convert_message(Message::Array(vec![
            Message::BulkStatic("REPLCONF"),
            Message::BulkStatic("GETACK"),
            Message::BulkStatic("*"),
        ]))
        .into_bytes();
        let senders = { self.senders.lock().unwrap().clone() };

        let mut receivers = Vec::new();
        for sender in senders {
            let (oneshot_sender, oneshot_receiver) = oneshot::channel();
            if sender
                .send((getack.clone(), Some(oneshot_sender)))
                .await
                .is_ok()
            {
                receivers.push(oneshot_receiver);
            }
        }
        return receivers;
    }
}

pub async fn run_forwarder(replica_connection: StreamReaderWriter, replicas: &Replicas) {
    // Split the connection so writes (SET propagation, GETACK) proceed
    // independently of reads (ACK collection). Without this, a missing ACK —
    // e.g. a WAIT timed out before this replica responded — would stall all
    // subsequent writes on this socket.
    let (mut replica_reader, mut replica_writer) = replica_connection.split();

    let (sender, mut receiver) =
        mpsc::channel::<(Vec<u8>, Option<oneshot::Sender<ReplyBytes>>)>(100);
    replicas.register(sender);

    // Send the RDB after registering. The replica considers the handshake
    // complete once it has received the RDB, so any SET that races the
    // handshake is guaranteed to find this replica already registered.
    let empty_rdb = stream_reader::encode_hex_rdb_file(stream_reader::EMPTY_RDB_HEX);
    replica_writer.write(&empty_rdb).await;

    // Replies are matched to GETACK requests in FIFO order — the replica's
    // replication stream is strictly ordered and each reply corresponds to
    // the next outstanding request.
    let mut pending: VecDeque<oneshot::Sender<ReplyBytes>> = VecDeque::new();

    loop {
        tokio::select! {
            // From a client connection task sending over a write or getack
            msg = receiver.recv() => {
                if let Some((bytes, oneshot_sender)) = msg {
                    replica_writer.write(&bytes).await;
                    if let Some(oneshot_sender) = oneshot_sender {
                        pending.push_back(oneshot_sender);
                    }
                }
            }
            // From the replica sending over a reply
            maybe_reply = replica_reader.get_message() => {
                if let Some((reply, _size)) = maybe_reply {
                    // Send the reply to all oneshots, since we may have a dead oneshot
                    // from a previously timed out wait in the queue still.
                    while let Some(os) = pending.pop_front() {
                        let _ = os.send(reply.clone());
                    }
                }
            }
        }
    }
}
