use std::io::{self, Write, stdin};
use std::path::PathBuf;

use sand_db::{Store, make_store};

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <directory>", args[0]);
        std::process::exit(1);
    }

    let directory = PathBuf::from(&args[1]);

    let mut store = match make_store(directory) {
        Ok(store) => store,

        Err(e) => {
            eprintln!("Failed to open store: {e}");
            std::process::exit(1);
        }
    };

    loop {
        print!("> ");
        std::io::stdout().flush()?;

        let mut cmd = String::new();
        stdin().read_line(&mut cmd)?;
        let cmd = cmd.trim();

        if cmd.is_empty() {
            continue;
        }

        let parts: Vec<&str> = cmd.split_whitespace().collect();

        match parts[0] {
            "exit" => {
                return Ok(());
            }

            "get" => {
                if parts.len() != 2 {
                    eprintln!("Usage: get <key>");
                    continue;
                }

                let key = parts[1];

                let value = match store.get(key) {
                    Ok(value) => value,

                    Err(e) => {
                        eprintln!("Failed to read key: {e}");
                        continue;
                    }
                };

                if let Some(value) = value {
                    eprintln!("{}", String::from_utf8_lossy(&value));
                } else {
                    eprintln!("Key not found");
                }
            }

            "set" => {
                if parts.len() != 3 {
                    eprintln!("Usage: set <key> <value>");
                    continue;
                }

                let key = parts[1];
                let value = parts[2];

                match store.insert(key, value.as_bytes()) {
                    Ok(_) => eprintln!("Key set"),
                    Err(e) => eprintln!("Failed to set key: {e}"),
                }
            }

            cmd => {
                eprintln!("Unknown command: {cmd}");
            }
        }
    }
}
