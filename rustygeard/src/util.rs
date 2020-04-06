use bytes::Bytes;

pub fn bytes2bool(input: &Bytes) -> bool {
    if input.len() != 1 {
        false
    } else if input[0] == b'1' {
        true
    } else {
        false
    }
}
