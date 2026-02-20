use std::io::{self, Write, stdin};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::ops::Bound;

use sand_db::{Store, make_store};

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <directory>", args[0]);
        std::process::exit(1);
    }

    let directory = PathBuf::from(&args[1]);

    let store = match make_store(directory) {
        Ok(store) => store,

        Err(e) => {
            eprintln!("Failed to open store: {e}");
            std::process::exit(1);
        }
    };

    let is_tty = stdin().is_terminal();

    'outer: loop {
        if is_tty {
            eprint!("> ");
        }

        std::io::stderr().flush()?;

        let mut cmd = String::new();
        stdin().read_line(&mut cmd)?;

        if cmd.is_empty() {
            return Ok(());
        }

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
                    let escaped: Vec<_> = value.iter()
                        .map(|it| std::ascii::escape_default(*it))
                        .flatten()
                        .collect();
                    eprintln!("{}", String::from_utf8_lossy(&escaped));
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

            "list" => {
                fn w_conflict() {
                    eprintln!(
                        "warning: conflicting filters; using values specified last."
                    );
                }

                fn usage() {
                    eprintln!("usage: list [(-gt|-lt|-lte|-gte) <value>]...", );
                }

                let mut args = parts.into_iter().skip(1);
                let mut range = (Bound::Unbounded, Bound::Unbounded);

                while let Some(op) = args.next() {
                    let operand = match args.next() {
                        Some(x) => x,
                        None => {
                            usage();
                            continue 'outer;
                        }
                    };

                    match op {
                        "-lt" => {
                            if range.1 != Bound::Unbounded {
                                w_conflict();
                            }
                            range.1 = Bound::Excluded(operand);
                        },
                        "-lte" => {
                            if range.1 != Bound::Unbounded {
                                w_conflict();
                            }
                            range.1 = Bound::Included(operand);
                        },
                        "-gt" => {
                            if range.0 != Bound::Unbounded {
                                w_conflict();
                            }
                            range.0 = Bound::Excluded(operand);
                        },
                        "-gte" => {
                            if range.0 != Bound::Unbounded {
                                w_conflict();
                            }
                            range.0 = Bound::Included(operand);
                        },

                        _ => {
                            usage();
                            continue 'outer;
                        }
                    }
                }

                match store.get_range(range) {
                    Ok(iter) => {
                        for item in iter {
                            let (key, value) = item?;
                            let escaped: Vec<u8> = value.iter()
                                .map(|it| std::ascii::escape_default(*it))
                                .flatten()
                                .collect();
                            let value = String::from_utf8_lossy(&escaped);

                            eprintln!("{key} => {value}");
                        }
                    }

                    Err(e) => eprintln!("Failed to list keys: {e}")
                }
            }

            cmd => {
                eprintln!("Unknown command: {cmd}");
            }
        }
    }
}
