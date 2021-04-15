use clap::{App, Arg};
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, AtomicI32, Ordering};
use std::sync::Arc;
use std::path::Path;
use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelIterator;

fn count_lines_file<P: AsRef<Path>>(path: P, total: Arc<AtomicUsize>) {
    if let Ok(file) = File::open(path) {
        let mut reader = BufReader::with_capacity(1024 * 32, file);
        let mut count = 0;
        loop {
            let len = {
                let buf = reader.fill_buf().unwrap();
                if buf.is_empty() {
                    break;
                }
                count += bytecount::count(&buf, b'\n');
                buf.len()
            };
            reader.consume(len);
        }
        total.fetch_add(count, Ordering::SeqCst);
    }
}

fn main() {
    let matches = App::new("lines")
        .version("1.0")
        .author("Axel Kappel <axel.e.kappel@gmail.com>")
        .about("Counts lines")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .multiple(true)
                .required(true),
        )
        .arg(
            Arg::with_name("recursive")
                .short("r")
                .help("Sets recursive"),
        )
        .get_matches();

    let recursive = matches.is_present("recursive");
    let total = Arc::new(AtomicUsize::new(0));
    let exit_code = Arc::new(AtomicI32::new(0));

    let iter = matches.values_of("INPUT").unwrap();
    let targets: Vec<_> = iter.collect();
    let par_file_iter = targets.into_par_iter();

    par_file_iter.for_each(|file| {
        let mut dir_stack = Vec::new();
        let mut files = Vec::new();

        let meta = match fs::metadata(file) {
            Ok(meta) => meta,
            _ => {
                println!("lines: {} No such file or directory", file);
                exit_code.store(1, Ordering::SeqCst);
                return;
            }
        };
    
        if meta.is_file() {
            count_lines_file(file, total.clone());
        } else {
            if recursive {
                let start = fs::read_dir(file).unwrap();
                for res in start {
                    let dir = res.unwrap();
                    if dir.metadata().unwrap().is_dir() {
                        dir_stack.push(dir);
                    } else {
                        files.push(dir.path());
                    }
                }
    
                while let Some(entry) = dir_stack.pop() {
                    let dirs = fs::read_dir(entry.path()).unwrap();
                    for res in dirs {
                        let dir = res.unwrap();
                        if dir.metadata().unwrap().is_dir() {
                            dir_stack.push(dir);
                        } else {
                            files.push(dir.path());
                        }
                    }
                }
    
                let par_iter = files.into_par_iter();
                let t = total.clone();
                par_iter.for_each(|path| count_lines_file(path, t.clone()));
            } else {
                println!("lines: {} Is a directory", file);
                exit_code.store(1, Ordering::SeqCst);
                return;
            }
        }
    });
    println!("Total length: {}", total.load(Ordering::SeqCst));
    std::process::exit(exit_code.load(Ordering::SeqCst));
}
