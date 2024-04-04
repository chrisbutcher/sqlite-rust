use anyhow::{bail, Error, Result};
use sqlite_starter_rust::{
    header::PageHeader,
    record::{self, parse_record},
    schema::Schema,
    types::*,
    varint,
};
use std::{
    env,
    fs::{read, File},
    io::{prelude::*, Cursor, SeekFrom},
    path::Path,
};

struct Record {
    row_id: usize,
    serial_types: Vec<SerialType>,
    serial_values: Vec<SerialValue>,
}

struct DatabaseHeader {
    page_size: u16,
    page_count: u32,
}

impl DatabaseHeader {
    pub fn new<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut header = [0; 100];
        reader.read_exact(&mut header)?;

        let page_size = u16::from_be_bytes([header[16], header[17]]);
        let page_count = u32::from_be_bytes([header[28], header[29], header[30], header[31]]);

        Ok(DatabaseHeader {
            page_size,
            page_count,
        })
    }
}

fn main() -> Result<()> {
    // TODO: Switch to clap
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db_file_path = Path::new(&args[1]);

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let (page_size, records) = read_records(db_file_path)?;

            println!("database page size: {}", page_size);
            println!("number of tables: {}", records.len());
        }
        ".tables" => {
            let (_page_size, records) = read_records(db_file_path)?;
            let table_names = get_table_names(&records).join(" ");

            println!("{table_names}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn read_records(file_name: &Path) -> anyhow::Result<(u16, Vec<Record>)> {
    let mut file = File::open(file_name)?;

    let database_header = DatabaseHeader::new(&mut file)?;

    // Reading root page
    let mut page_bytes = vec![0; database_header.page_size as usize - 100];
    file.read_exact(&mut page_bytes)?;

    let mut page_cursor = Cursor::new(&page_bytes);
    page_cursor.seek(SeekFrom::Start(0))?;

    let mut page_header_bytes = [0; 8];
    page_cursor.read_exact(&mut page_header_bytes)?;

    let page_header = PageHeader::parse(&page_header_bytes)?;

    let cell_pointers = build_cell_pointers(&page_header, &mut page_cursor)?;

    let payloads = build_payloads(&page_header, cell_pointers, &mut page_cursor)?;

    match page_header.page_type {
        sqlite_starter_rust::header::BTreePage::LeafTable => {
            let records = build_records(payloads)?;

            Ok((database_header.page_size, records))
        }
        _ => todo!(
            "handle other page types ({:?}) in read_records",
            page_header.page_type
        ),
    }
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
    page_header: &PageHeader,
    cell_pointers: Vec<u16>,
    reader: &mut R,
) -> anyhow::Result<Vec<(usize, usize, Vec<u8>)>> {
    match page_header.page_type {
        sqlite_starter_rust::header::BTreePage::LeafTable => {
            let mut payloads = vec![];

            for offset in cell_pointers {
                reader.seek(SeekFrom::Start(offset as u64 - 100))?; // TODO vary this behavior for pages other than page 1

                let (payload_size, _bytes_read_1) = varint::parse_varint_from_reader(reader);
                let (row_id, _bytes_read_2) = varint::parse_varint_from_reader(reader);

                let mut payload_bytes = vec![0; payload_size];
                reader.read_exact(&mut payload_bytes)?;

                payloads.push((payload_size, row_id, payload_bytes));
            }

            Ok(payloads)
        }
        _ => todo!(
            "handle other page types ({:?}) in build_payloads",
            page_header.page_type
        ),
    }
}

fn build_cell_pointers<R: Read>(
    page_header: &PageHeader,
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
