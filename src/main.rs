use anyhow::{bail, Result};
use sqlite_starter_rust::{header::PageHeader, types::*, varint};
use std::{
    fs::File,
    io::{prelude::*, Cursor, SeekFrom},
    path::Path,
};

#[allow(dead_code)]
#[derive(Debug)]
struct Record {
    row_id: usize,
    serial_types: Vec<SerialType>,
    serial_values: Vec<SerialValue>,
}

#[allow(dead_code)]
struct Database {
    page_size: u32,
    page_count: u32,
    database_file: File,
}

impl Database {
    pub fn open(mut database_file: File) -> anyhow::Result<Self> {
        let mut header = [0; 100];
        database_file.read_exact(&mut header)?;

        let mut page_size = u16::from_be_bytes([header[16], header[17]]) as u32;

        if page_size == 1 {
            // If page_size is 1, this should be interpreted as 65,536
            page_size = 65_536;
        }

        let page_count = u32::from_be_bytes([header[28], header[29], header[30], header[31]]);

        Ok(Database {
            page_size,
            page_count,
            database_file,
        })
    }

    pub fn seek_to_page(&mut self, page_num: u32) -> anyhow::Result<Page> {
        // TODO: Error if page num is out of range.
        let mut seek_offset = (page_num - 1) * self.page_size;

        if page_num == 1 {
            // Skip first 100 bytes of page 1 to account for the database header.
            seek_offset += 100;
        }

        self.database_file
            .seek(SeekFrom::Start(seek_offset as u64))?;

        let mut page_header_bytes = [0; 8];
        self.database_file.read_exact(&mut page_header_bytes)?;
        let header = PageHeader::parse(&page_header_bytes)?;

        Ok(Page { header })
    }
}

#[derive(Debug)]
struct Page {
    header: PageHeader,
}

impl Page {
    pub fn fetch_cell_pointers<R: Read + std::io::Seek>(
        &self,
        reader: &mut R,
    ) -> anyhow::Result<Vec<u16>> {
        let cell_pointers = Self::build_cell_pointers(&self.header, reader)?;

        Ok(cell_pointers)
    }

    fn build_cell_pointers<R: Read>(
        page_header: &PageHeader,
        reader: &mut R,
    ) -> anyhow::Result<Vec<u16>> {
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
}

// TODO:
// * Error on unhandled cases like overflow page needed.
// * Detect which pages are root pages based on master table.
// * Assume root page is table leaf page for now, but handle if it's not.
fn main() -> Result<()> {
    // TODO: Switch to clap
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db_file_path = Path::new(&args[1]);
    let db_file = File::open(db_file_path)?;
    let database = Database::open(db_file)?;

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let (page_size, records) = read_records(database)?;

            println!("database page size: {}", page_size);
            println!("number of tables: {}", records.len());
        }
        ".tables" => {
            let (_page_size, records) = read_records(database)?;
            let table_names = get_table_names(&records).join(" ");

            println!("{table_names}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn read_records(mut database: Database) -> anyhow::Result<(u32, Vec<Record>)> {
    let page = database.seek_to_page(1)?;
    let cell_pointers = page.fetch_cell_pointers(&mut database.database_file)?;

    let payloads = build_payloads(&page, &cell_pointers, &mut database.database_file)?;

    for page_i in 2..=database.page_count {
        let next_page = database.seek_to_page(page_i)?;
        println!("page #{page_i}: {:?}", next_page);
    }

    match page.header.page_type {
        sqlite_starter_rust::header::BTreePage::LeafTable => {
            let records = build_records(payloads)?;

            Ok((database.page_size, records))
        }
        _ => todo!(
            "handle other page types ({:?}) in read_records",
            page.header.page_type
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
    page: &Page,
    cell_pointers: &Vec<u16>,
    reader: &mut R,
) -> anyhow::Result<Vec<(usize, usize, Vec<u8>)>> {
    match page.header.page_type {
        sqlite_starter_rust::header::BTreePage::LeafTable => {
            let mut payloads = vec![];

            for offset in cell_pointers {
                reader.seek(SeekFrom::Start(*offset as u64))?;

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
            page.header.page_type
        ),
    }
}
