// Message enum for return types
#[derive(Clone)]
pub enum Message {
    Simple(String),
    SimpleStatic(&'static str),
    Bulk(String),
    BulkStatic(&'static str),
    Err(&'static str),
    Array(Vec<Message>),
    Integer(i64),
    NullBulk,
    NullArray,
}

pub fn convert_message(message: Message) -> String {
    match message {
        Message::Simple(message) => format!("+{}\r\n", message),
        Message::SimpleStatic(message) => format!("+{}\r\n", message),
        Message::Bulk(message) => format!("${}\r\n{}\r\n", message.len(), message),
        Message::BulkStatic(message) => format!("${}\r\n{}\r\n", message.len(), message),
        Message::Err(message) => format!("-ERR {}\r\n", message),
        Message::Array(message) => {
            let mut result = format!("*{}\r\n", message.len());
            for inner_message in message {
                result.push_str(&convert_message(inner_message));
            }
            result
        }
        Message::Integer(int) => format!(":{}\r\n", int),
        Message::NullBulk => "$-1\r\n".to_string(),
        Message::NullArray => "*-1\r\n".to_string(),
    }
}

// Option<String> so we can take from it
pub fn parse_message(buffer: &[u8]) -> (Vec<Option<String>>, usize) {
    return match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => panic!("Unexepcted first character: {}", buffer[0] as char),
    };
}

fn parse_simple_string(buffer: &[u8]) -> (Vec<Option<String>>, usize) {
    let next_start = get_crlf_end_pos(buffer);
    let word = String::from_utf8(buffer[1..next_start - 2].to_vec()).unwrap();
    return (vec![Some(word)], next_start);
}

fn parse_bulk_string(buffer: &[u8]) -> (Vec<Option<String>>, usize) {
    let start_of_word = get_crlf_end_pos(buffer);
    let next_start = get_crlf_end_pos(&buffer[start_of_word..]) + start_of_word;
    let word = String::from_utf8(buffer[start_of_word..next_start - 2].to_vec()).unwrap();
    return (vec![Some(word)], next_start);
}

fn parse_array(buffer: &[u8]) -> (Vec<Option<String>>, usize) {
    let mut next_start = get_crlf_end_pos(buffer);
    let array_len = String::from_utf8(buffer[1..next_start - 2].to_vec())
        .unwrap()
        .parse()
        .unwrap();
    let mut array = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        let (mut message, message_end) = parse_message(&buffer[next_start..]);
        array.append(&mut message);
        next_start += message_end;
    }
    return (array, next_start);
}

fn get_crlf_end_pos(buffer: &[u8]) -> usize {
    return buffer
        .windows(2)
        .position(|w| w == b"\r\n")
        .expect("Invalid message format: missing CRLF")
        + 2;
}
