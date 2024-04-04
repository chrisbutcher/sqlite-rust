use anyhow::Error;
use std::io::Read;

#[derive(Debug, PartialEq)]
pub enum SerialType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float,
    Zero,
    One,
    Blob(u64),
    String(u64),
}
impl SerialType {
    pub fn from(raw_serial_type: u64) -> SerialType {
        match raw_serial_type {
            0 => SerialType::Null,
            1 => SerialType::Int8,
            2 => SerialType::Int16,
            3 => SerialType::Int24,
            4 => SerialType::Int32,
            5 => SerialType::Int48,
            6 => SerialType::Int64,
            7 => SerialType::Float,
            8 => SerialType::Zero,
            9 => SerialType::One,
            10 | 11 => todo!(),
            n => {
                if n % 2 == 0 {
                    SerialType::Blob((n - 12) / 2)
                } else {
                    SerialType::String((n - 13) / 2)
                }
            }
        }
    }
}
#[derive(Debug, PartialEq)]
pub enum SerialValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float(f64),
    Zero,
    One,
    Blob(Vec<u8>),
    String(String),
}
impl SerialValue {
    pub fn parse<R: Read>(
        reader: &mut R,
        serial_type: &SerialType,
    ) -> anyhow::Result<SerialValue, Error> {
        match serial_type {
            SerialType::Null => Ok(SerialValue::Null),
            SerialType::Int8 => {
                let mut buf = [0; 1];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Int8(i8::from_be_bytes(buf)))
            }
            SerialType::Int16 => {
                let mut buf = [0; 2];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Int16(i16::from_be_bytes(buf)))
            }
            SerialType::Int24 => {
                let mut buf = [0; 3];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Int24(i32::from_be_bytes([
                    0, buf[0], buf[1], buf[2],
                ])))
            }
            SerialType::Int32 => {
                let mut buf = [0; 4];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Int32(i32::from_be_bytes(buf)))
            }
            SerialType::Int48 => {
                let mut buf = [0; 6];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Int48(i64::from_be_bytes([
                    0, 0, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5],
                ])))
            }
            SerialType::Int64 => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Int64(i64::from_be_bytes(buf)))
            }
            SerialType::Float => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Float(f64::from_be_bytes(buf)))
            }
            SerialType::Zero => Ok(SerialValue::Zero),
            SerialType::One => Ok(SerialValue::One),
            SerialType::Blob(size) => {
                let mut buf = vec![0; *size as usize];
                reader.read_exact(&mut buf)?;

                Ok(SerialValue::Blob(buf))
            }
            SerialType::String(size) => {
                let mut buf = vec![0; *size as usize];
                reader.read_exact(&mut buf)?;
                let value = String::from_utf8(buf)?;

                Ok(SerialValue::String(value))
            }
        }
    }
}
