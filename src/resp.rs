// Message enum for return types
pub enum Message {
    Simple(String),
    SimpleStatic(&'static str),
    Bulk(String),
    Array(Vec<Option<String>>),
    NullBulk(),
}

pub fn convert_message(message: Message) -> String {
    match message {
        Message::Simple(message) => format!("+{}\r\n", message),
        Message::SimpleStatic(message) => format!("+{}\r\n", message),
        Message::Bulk(message) => format!("${}\r\n{}\r\n", message.len(), message),
        Message::Array(message) => {
            let mut result = format!("*{}\r\n", message.len());
            for word in message {
                result.push_str(&convert_message(Message::Bulk(word.unwrap())));
            }
            result
        }
        Message::NullBulk() => "$-1\r\n".to_string(),
    }
}

// Option<String> so we can take from it
pub fn parse_message(buffer: &[u8]) -> Option<(Vec<Option<String>>, usize)> {
    return match *buffer.get(0)? as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => None,
    };
}

fn parse_simple_string(buffer: &[u8]) -> Option<(Vec<Option<String>>, usize)> {
    let (line, next_start) = up_to_break(&buffer[1..], 1)?;
    return Some((vec![Some(line)], next_start + 1));
}

fn parse_bulk_string(buffer: &[u8]) -> Option<(Vec<Option<String>>, usize)> {
    let (_, word_start) = up_to_break(&buffer[1..], 1)?;
    let (word, next_start) = up_to_break(&buffer[word_start..], word_start)?;
    return Some((vec![Some(word)], next_start));
}

fn parse_array(buffer: &[u8]) -> Option<(Vec<Option<String>>, usize)> {
    let (line, mut start) = up_to_break(&buffer[1..], 1)?;
    let array_len: usize = line.parse().ok()?;
    let mut array = Vec::with_capacity(array_len);
    for _ in 0..array_len {
        let (mut message, next_start) = parse_message(&buffer[start..])?;
        array.append(&mut message);
        start += next_start;
    }
    return Some((array, start));
}

fn up_to_break(buffer: &[u8], real_start: usize) -> Option<(String, usize)> {
    for i in 1..buffer.len() {
        if buffer.get(i - 1)? == &b'\r' && buffer.get(i)? == &b'\n' {
            return Some((
                String::from_utf8(buffer[0..i - 1].to_vec()).ok()?,
                real_start + i + 1,
            ));
        }
    }
    return None;
}
