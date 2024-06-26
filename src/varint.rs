use std::io::Read;

const IS_FIRST_BIT_ZERO_MASK: u8 = 0b10000000;
const LAST_SEVEN_BITS_MASK: u8 = 0b01111111;

/// Parses SQLite's "varint" (short for variable-length integer) as mentioned here:
/// [varint](https://www.sqlite.org/fileformat2.html#varint)
///
/// Returns (varint, bytes_read)
pub fn parse_varint(stream: &[u8]) -> (usize, usize) {
    let usable_bytes = read_usable_bytes(stream);
    let bytes_read = usable_bytes.len();

    let varint = usable_bytes
        .into_iter()
        .enumerate()
        .fold(0, |value, (i, usable_byte)| {
            let usable_size = if i == 8 { 8 } else { 7 };
            (value << usable_size) + usable_value(usable_size, usable_byte) as usize
        });
    (varint, bytes_read)
}

pub fn parse_varint_from_reader<R: Read>(reader: &mut R) -> (usize, usize) {
    let usable_bytes = read_usable_bytes_from_reader(reader);

    let bytes_read = usable_bytes.len();
    let varint = usable_bytes
        .into_iter()
        .enumerate()
        .fold(0, |value, (i, usable_byte)| {
            let usable_size = if i == 8 { 8 } else { 7 };

            (value << usable_size) + usable_value(usable_size, usable_byte) as usize
        });

    (varint, bytes_read)
}

/// Usable size is either 8 or 7
fn usable_value(usable_size: u8, byte: u8) -> u8 {
    if usable_size == 8 {
        usable_size
    } else {
        byte & LAST_SEVEN_BITS_MASK
    }
}

fn read_usable_bytes(stream: &[u8]) -> Vec<u8> {
    let mut usable_bytes = vec![];

    for i in 0..9 {
        let byte = stream[i];
        usable_bytes.push(byte);
        if starts_with_zero(byte) {
            break;
        }
    }

    usable_bytes
}

fn read_usable_bytes_from_reader<R: Read>(reader: &mut R) -> Vec<u8> {
    let mut usable_bytes = vec![];

    for _i in 0..9 {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).unwrap();

        usable_bytes.push(byte[0]);
        if starts_with_zero(byte[0]) {
            break;
        }
    }

    usable_bytes
}

fn starts_with_zero(byte: u8) -> bool {
    (byte & IS_FIRST_BIT_ZERO_MASK) == 0
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_parse_varint() {
        let a = [92, 4, 7, 23, 33, 33, 1, 129, 3, 116];

        let mut total_bytes_read = 0;

        let (num, bytes_read) = parse_varint(&a[total_bytes_read..]);
        total_bytes_read += bytes_read;
        assert_eq!(num, 92);
        assert_eq!(bytes_read, 1);

        let (num, bytes_read) = parse_varint(&a[total_bytes_read..]);
        total_bytes_read += bytes_read;
        assert_eq!(num, 4);
        assert_eq!(bytes_read, 1);

        for _ in 0..5 {
            let (_num, bytes_read) = parse_varint(&a[total_bytes_read..]);
            total_bytes_read += bytes_read;
        }

        // NOTE: Consecutive bytes `129, 3` are read as 131.
        let (num, bytes_read) = parse_varint(&a[total_bytes_read..]);
        total_bytes_read += bytes_read;
        assert_eq!(num, 131);
        assert_eq!(bytes_read, 2);

        let (num, bytes_read) = parse_varint(&a[total_bytes_read..]);
        assert_eq!(num, 116);
        assert_eq!(bytes_read, 1);
    }

    #[test]
    fn test_parse_varint_from_reader() {
        let a = vec![92, 4, 7, 23, 33, 33, 1, 129, 3, 116];

        let mut c = Cursor::new(a);

        let (num, bytes_read) = parse_varint_from_reader(&mut c);
        assert_eq!(num, 92);
        assert_eq!(bytes_read, 1);

        let (num, bytes_read) = parse_varint_from_reader(&mut c);
        assert_eq!(num, 4);
        assert_eq!(bytes_read, 1);

        for _ in 0..5 {
            parse_varint_from_reader(&mut c);
        }

        // NOTE: Consecutive bytes `129, 3` are read as 131.
        let (num, bytes_read) = parse_varint_from_reader(&mut c);
        assert_eq!(num, 131);
        assert_eq!(bytes_read, 2);

        let (num, bytes_read) = parse_varint_from_reader(&mut c);
        assert_eq!(num, 116);
        assert_eq!(bytes_read, 1);
    }
}
