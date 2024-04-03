use anyhow::{bail, Error, Result};
use itertools::Itertools;
use sqlite_starter_rust::{
    header::PageHeader,
    record::{self, parse_record},
    schema::Schema,
    varint,
};
use std::{
    env,
    fs::{read, File},
    io::{prelude::*, Cursor, SeekFrom},
};

#[derive(Debug, PartialEq)]
enum SerialType {
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
    fn from(header_value: u64) -> SerialType {
        match header_value {
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
enum SerialValue {
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
    fn parse<R: Read>(
        reader: &mut R,
        serial_type: &SerialType,
    ) -> anyhow::Result<SerialValue, Error> {
        match serial_type {
            SerialType::Null => Ok(SerialValue::Null),
            SerialType::Int8 => {
                let mut buffer = [0; 1];
                reader.read_exact(&mut buffer)?;
                Ok(SerialValue::Int8(i8::from_be_bytes(buffer)))
            }
            SerialType::Int16 => {
                let mut buffer = [0; 2];
                reader.read_exact(&mut buffer)?;
                Ok(SerialValue::Int16(i16::from_be_bytes(buffer)))
            }
            SerialType::Int24 => {
                let mut buffer = [0; 3];
                reader.read_exact(&mut buffer)?;
                // here we need to mark
                Ok(SerialValue::Int24(i32::from_be_bytes([
                    0, buffer[0], buffer[1], buffer[2],
                ])))
            }
            SerialType::Int32 => {
                let mut buffer = [0; 4];
                reader.read_exact(&mut buffer);
                Ok(SerialValue::Int32(i32::from_be_bytes(buffer)))
            }
            SerialType::Int48 => {
                let mut buffer = [0; 6];
                reader.read_exact(&mut buffer)?;
                Ok(SerialValue::Int48(i64::from_be_bytes([
                    0, 0, buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5],
                ])))
            }
            SerialType::Int64 => {
                let mut buffer = [0; 8];
                reader.read_exact(&mut buffer)?;
                Ok(SerialValue::Int64(i64::from_be_bytes(buffer)))
            }
            SerialType::Float => {
                let mut buffer = [0; 8];
                reader.read_exact(&mut buffer)?;
                Ok(SerialValue::Float(f64::from_be_bytes(buffer)))
            }
            SerialType::Zero => Ok(SerialValue::Zero),
            SerialType::One => Ok(SerialValue::One),
            SerialType::Blob(size) => {
                let mut buffer = vec![0; size.clone() as usize];
                reader.read_exact(&mut buffer)?;
                Ok(SerialValue::Blob(buffer))
            }
            SerialType::String(size) => {
                let mut buffer = vec![0; size.clone() as usize];
                reader.read_exact(&mut buffer)?;
                let value = String::from_utf8(buffer)?;
                Ok(SerialValue::String(value))
            }
        }
    }
}

struct Record {
    row_id: usize,
    serial_types: Vec<SerialType>,
    serial_values: Vec<SerialValue>,
}

fn main() -> Result<()> {
    let path = env::current_dir()?;

    // TODO: Switch to clap
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let (page_size, records) = read_records(&args[1])?;
            println!("database page size: {}", page_size);
            println!("number of tables: {}", records.len());
        }
        ".tables" => {
            let (_page_size, records) = read_records(&args[1])?;
            let table_names = get_table_names(&records).join(" ");

            println!("{table_names}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

// TODO: Use Path
fn read_records(file_name: &str) -> anyhow::Result<(u16, Vec<Record>)> {
    let mut file = File::open(file_name)?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    #[allow(unused_variables)]
    let page_size = u16::from_be_bytes([header[16], header[17]]);

    file.rewind()?;

    // Reading root page
    let mut page_bytes = vec![0; page_size as usize + 100];
    file.read_exact(&mut page_bytes)?;

    let mut page_cursor = Cursor::new(&page_bytes);
    page_cursor.seek(SeekFrom::Start(100))?;

    let mut page_header_bytes = [0; 8];
    page_cursor.read_exact(&mut page_header_bytes)?;

    let page_header = PageHeader::parse(&page_header_bytes)?;

    let cell_pointers = build_cell_pointers(page_header, &mut page_cursor)?;

    // TODO: Add payload struct
    let payloads = build_payloads(cell_pointers, &mut page_cursor)?;

    let records = build_records(payloads)?;

    Ok((page_size, records))
}

fn get_table_names(records: &Vec<Record>) -> Vec<String> {
    let mut result = vec![];

    for record in records {
        let table_name_val = &record.serial_values[1];

        match &table_name_val {
            SerialValue::String(s) => result.push(s.to_string()),
            _ => panic!(
                "reading root page table name failed with table_name_val: {:?}",
                table_name_val
            ),
        }
    }

    result
}

fn build_records(payloads: Vec<(usize, usize, Vec<u8>)>) -> anyhow::Result<Vec<Record>> {
    let mut records = vec![];

    for (_payload_size, row_id, payload_bytes) in payloads {
        let mut payload_cursor = Cursor::new(payload_bytes);

        let mut serial_types: Vec<SerialType> = vec![];
        let mut serial_values = vec![];

        let (record_header_byte_count, bytes_read_3) =
            varint::parse_varint_from_reader(&mut payload_cursor);

        let mut record_header_bytes_remaining = record_header_byte_count - bytes_read_3;

        loop {
            let (column_serial_type, col_type_bytes_read) =
                varint::parse_varint_from_reader(&mut payload_cursor);

            let serial_type = SerialType::from(column_serial_type as u64);

            serial_types.push(serial_type);

            record_header_bytes_remaining -= col_type_bytes_read;

            if record_header_bytes_remaining == 0 {
                break;
            }
        }

        for column_serial_type in &serial_types {
            let serial_value = SerialValue::parse(&mut payload_cursor, column_serial_type)?;

            serial_values.push(serial_value);
        }

        records.push(Record {
            row_id,
            serial_types,
            serial_values,
        });
    }

    Ok(records)
}

fn build_payloads<R: Read + std::io::Seek>(
    cell_pointers: Vec<u16>,
    reader: &mut R,
) -> anyhow::Result<Vec<(usize, usize, Vec<u8>)>> {
    let mut payloads = vec![];

    for offset in cell_pointers {
        reader.seek(SeekFrom::Start(offset as u64))?;

        let (payload_size, _bytes_read_1) = varint::parse_varint_from_reader(reader);
        let (row_id, _bytes_read_2) = varint::parse_varint_from_reader(reader);

        let mut payload_bytes = vec![0; payload_size];
        reader.read_exact(&mut payload_bytes)?;

        payloads.push((payload_size, row_id, payload_bytes));
    }

    Ok(payloads)
}

fn build_cell_pointers<R: Read>(
    page_header: PageHeader,
    reader: &mut R,
) -> anyhow::Result<Vec<u16>> {
    // TODO branch on page_header.page_type. For now, assume it's a table leaf cell.
    let mut cell_pointers = Vec::with_capacity(page_header.number_of_cells.into());
    let mut cell_pointer_buffer = [0; 2];
    for _ in 0..page_header.number_of_cells {
        reader.read_exact(&mut cell_pointer_buffer)?;
        cell_pointers.push(u16::from_be_bytes([
            cell_pointer_buffer[0],
            cell_pointer_buffer[1],
        ]))
    }

    Ok(cell_pointers)
}
