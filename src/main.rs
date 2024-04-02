use anyhow::{bail, Result};
use itertools::Itertools;
use sqlite_starter_rust::{header::PageHeader, record::parse_record, schema::Schema, varint};
use std::{
    fs::File,
    io::{prelude::*, Cursor, SeekFrom},
};

fn main() -> Result<()> {
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
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            file.rewind()?;

            // Reading root page
            let mut page_bytes = vec![0; page_size as usize + 100];
            file.read_exact(&mut page_bytes)?;

            // TODO: Read from file instead of loading full page into memory?
            let mut page_cursor = Cursor::new(&page_bytes);
            page_cursor.seek(SeekFrom::Start(100))?;

            let mut page_header_bytes = [0; 8];
            page_cursor.read_exact(&mut page_header_bytes)?;

            let page_header = PageHeader::parse(&page_header_bytes)?;

            println!("number_of_cells: {}", page_header.number_of_cells);
            println!(
                "start_of_content_area: {}",
                page_header.start_of_content_area
            );

            // TODO branch on page_header.page_type. For now, assume it's a table leaf cell.
            let mut cell_pointers = Vec::with_capacity(page_header.number_of_cells.into());
            let mut cell_pointer_buffer = [0; 2];
            for _ in 0..page_header.number_of_cells {
                page_cursor.read_exact(&mut cell_pointer_buffer)?;
                cell_pointers.push(u16::from_be_bytes([
                    cell_pointer_buffer[0],
                    cell_pointer_buffer[1],
                ]))
            }

            // for (i, page_byte) in page_bytes.iter().enumerate() {
            //     println!("{i}: {page_byte}");
            // }

            for offset in cell_pointers {
                page_cursor.seek(SeekFrom::Start(offset as u64))?;

                let (payload_size, bytes_read_1) =
                    varint::parse_varint_from_reader(&mut page_cursor);
                let (row_id, bytes_read_2) = varint::parse_varint_from_reader(&mut page_cursor);

                let mut payload_bytes = vec![0; payload_size];
                page_cursor.read_exact(&mut payload_bytes)?;

                println!("payload_size: {payload_size}");
                println!("row_id: {row_id}");
                println!("payload_bytes: {:?}", payload_bytes);

                // let record = parse_record(&payload_bytes, 6)?;
                // println!("record: {:?}", record);

                // let schema = Schema::parse(record).unwrap();
                // println!("schema: {:?}", schema);
            }

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
