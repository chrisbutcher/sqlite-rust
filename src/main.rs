use anyhow::{bail, Result};
use sqlite_starter_rust::header::PageHeader;
use std::{
    fs::File,
    io::{prelude::*, SeekFrom},
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

            let mut page_header = [0; 8];
            file.read_exact(&mut page_header)?;
            let page_header = PageHeader::parse(&page_header)?;

            println!("page_header: {:?}", page_header);

            let mut cell_content_addresses = vec![];

            for c in 0..page_header.number_of_cells {
                println!("c: {c}");

                let mut cell_pointer = [0; 2];
                file.read_exact(&mut cell_pointer)?;

                let cell_content_address = u16::from_be_bytes(cell_pointer[0..2].try_into()?);

                cell_content_addresses.push(cell_content_address);
            }

            println!("cell_content_addresses: {:?}", cell_content_addresses);

            // file.rewind()?; // TODO: avoid rewind?

            // for cell_content_address in &cell_content_addresses {
            //     file.seek(SeekFrom::Start(*cell_content_address as u64))?;

            //     let mut foo = [0; 9];
            // }

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
