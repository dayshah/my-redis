use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::resp;

pub struct StreamReaderWriter {
    stream: TcpStream,
    // Heap-allocated so the 1 MiB buffer doesn't inflate async state machines.
    buf: Vec<u8>,
    cursor: usize,
    last_read_size: Option<usize>,
}

pub struct StreamReader {
    stream: OwnedReadHalf,
    buf: Vec<u8>,
    cursor: usize,
    last_read_size: Option<usize>,
}

pub struct StreamWriter {
    stream: OwnedWriteHalf,
}

impl StreamReaderWriter {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: vec![0; 1024 * 1024],
            cursor: 0,
            last_read_size: None,
        }
    }

    /// Split into independent read and write halves, preserving any bytes
    /// already buffered on the read side.
    pub fn split(self) -> (StreamReader, StreamWriter) {
        let (read_half, write_half) = self.stream.into_split();
        (
            StreamReader {
                stream: read_half,
                buf: self.buf,
                cursor: self.cursor,
                last_read_size: self.last_read_size,
            },
            StreamWriter { stream: write_half },
        )
    }

    pub async fn get_message(&mut self) -> Option<(Vec<Option<String>>, usize)> {
        let last_read_size = if let Some(last_read_size) = self.last_read_size {
            last_read_size
        } else {
            let last_read_size = self.stream.read(&mut self.buf).await.unwrap_or(0);
            if last_read_size == 0 {
                return None;
            }
            self.last_read_size = Some(last_read_size);
            last_read_size
        };
        let (message, bytes_parsed) = resp::parse_message(&self.buf[self.cursor..]);
        self.cursor += bytes_parsed;
        if self.cursor >= last_read_size {
            // Nothing left to read from this buffer so read again next time
            self.last_read_size = None;
            self.cursor = 0;
        }
        return Some((message, bytes_parsed));
    }

    pub async fn read_rdb_file(&mut self) {
        let last_read_size = if let Some(last_read_size) = self.last_read_size {
            last_read_size
        } else {
            let last_read_size = self.stream.read(&mut self.buf).await.unwrap();
            self.last_read_size = Some(last_read_size);
            last_read_size
        };
        let (_, size) = decode_hex_rdb_file(&self.buf[self.cursor..last_read_size]);
        self.cursor += size;
        if self.cursor >= last_read_size {
            self.last_read_size = None;
            self.cursor = 0;
        }
    }

    pub async fn write(&mut self, bytes_to_write: &[u8]) {
        self.stream.write_all(bytes_to_write).await.unwrap();
    }
}

impl StreamReader {
    pub async fn get_message(&mut self) -> Option<(Vec<Option<String>>, usize)> {
        let last_read_size = if let Some(last_read_size) = self.last_read_size {
            last_read_size
        } else {
            let last_read_size = self.stream.read(&mut self.buf).await.unwrap_or(0);
            if last_read_size == 0 {
                return None;
            }
            self.last_read_size = Some(last_read_size);
            last_read_size
        };
        let (message, bytes_parsed) = resp::parse_message(&self.buf[self.cursor..]);
        self.cursor += bytes_parsed;
        if self.cursor >= last_read_size {
            self.last_read_size = None;
            self.cursor = 0;
        }
        return Some((message, bytes_parsed));
    }
}

impl StreamWriter {
    pub async fn write(&mut self, bytes_to_write: &[u8]) {
        self.stream.write_all(bytes_to_write).await.unwrap();
    }
}

fn decode_hex_rdb_file(data: &[u8]) -> (String, usize) {
    let header_end = data
        .windows(2)
        .position(|w| w == b"\r\n")
        .expect("Invalid RDB format: missing CRLF")
        + 2;

    let len_str = str::from_utf8(&data[1..header_end - 2]).unwrap();
    let len = len_str.parse::<usize>().unwrap();
    let start = header_end;
    let end = start + len;
    let content = &data[start..end];

    let mut res = String::with_capacity(content.len() * 2);
    for byte in content {
        use std::fmt::Write;
        write!(&mut res, "{:02x}", byte).unwrap();
    }
    return (res, end);
}

pub fn encode_hex_rdb_file(file_str: &str) -> Vec<u8> {
    let hex_len = file_str.len() / 2;
    let mut res = format!("${}\r\n", hex_len).into_bytes();
    for i in (0..file_str.len()).step_by(2) {
        res.push(u8::from_str_radix(&file_str[i..i + 2], 16).unwrap());
    }
    return res;
}

pub static EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
