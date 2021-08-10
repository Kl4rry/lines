use std::{fs, fs::File, io::{BufRead, BufReader}, path::{Path, PathBuf}, sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc,
    }};

use clap::{App, Arg};
use rayon::{
    iter::{IntoParallelIterator, ParallelIterator},
    ThreadPoolBuilder, ThreadPool
};

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
        total.fetch_add(count, Ordering::Relaxed);
    }
}

fn count_lines_async(path: PathBuf, total: Arc<AtomicUsize>, pool: Arc<ThreadPool>) {
    pool.install(|| {
        count_lines_file(path, total);
    });
}

fn count_lines_dir(path: PathBuf, total: Arc<AtomicUsize>, pool: Arc<ThreadPool>) {
    pool.install(|| {
        let dirs = fs::read_dir(path).unwrap();
        for res in dirs {
            let dir = res.unwrap();
            if dir.metadata().unwrap().is_dir() {
                count_lines_dir(dir.path(), total.clone(), pool.clone());
            } else {
                count_lines_async(dir.path(), total.clone(), pool.clone());
            }
        }
    });
}

fn count() -> i32 {
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

    
    let cpus = num_cpus::get();
    let pool = Arc::new(ThreadPoolBuilder::new().num_threads(cpus * 2).build().unwrap());

    let recursive = matches.is_present("recursive");
    let total = Arc::new(AtomicUsize::new(0));
    let exit_code = Arc::new(AtomicI32::new(0));

    let iter = matches.values_of("INPUT").unwrap();
    let targets: Vec<_> = iter.collect();
    let par_file_iter = targets.into_par_iter();

    par_file_iter.for_each(|file| {
        let meta = match fs::metadata(file) {
            Ok(meta) => meta,
            _ => {
                println!("lines: {} No such file or directory", file);
                exit_code.store(1, Ordering::Relaxed);
                return;
            }
        };

        if meta.is_file() {
            count_lines_async(file.into(), total.clone(), pool.clone());
        } else {
            if recursive {
                count_lines_dir(file.into(), total.clone(), pool.clone());
            } else {
                println!("lines: {} Is a directory", file);
                exit_code.store(1, Ordering::Relaxed);
                return;
            }
        }
    });

    drop(pool);

    if total.load(Ordering::Relaxed) != 0 {
        println!("{}", total.load(Ordering::Relaxed));
    }

    return exit_code.load(Ordering::Relaxed);
}

fn main() {
    std::process::exit(count());
}
