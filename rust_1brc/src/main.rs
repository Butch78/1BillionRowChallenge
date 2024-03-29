use polars::prelude::*;
use std::time::Instant;
use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
};

mod purple_mist;
mod rangnargrootkeorkamp;
mod thebracket;

/// This code has been sourced from here:
/// https://github.com/coriolinus/1brc/tree/main

/// Size of chunk that each thread will process at a time
const CHUNK_SIZE: u64 = 16 * 1024 * 1024;
/// How much extra space we back the chunk start up by, to ensure we capture the full initial record
///
/// Must be greater than the longest line in the table
const CHUNK_EXCESS: u64 = 64;

type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy)]
struct Records {
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
}

impl Records {
    fn update(&mut self, item: f32) {
        self.count += 1;
        self.min = self.min.min(item);
        self.max = self.max.max(item);
        self.sum += item;
    }

    fn from_item(item: f32) -> Self {
        Self {
            count: 1,
            min: item,
            max: item,
            sum: item,
        }
    }

    fn mean(&self) -> f32 {
        let mean = self.sum / (self.count as f32);
        (mean * 10.0).round() / 10.0
    }

    fn merge(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            sum: self.sum + other.sum,
        }
    }
}

type Map = std::collections::HashMap<String, Records>;
// note that we defer parsing the slice into a string until as late as possible, which hopefully
// minimizes access time
type BorrowedMap<'a> = std::collections::HashMap<&'a [u8], Records>;

/// Get an aligned buffer from the given file.
///
/// "Aligned" in this case means that the first byte of the returned buffer is the
/// first byte of a record, and if `offset != 0` then the previous byte of the source file is `\n`,
/// and the final byte of the returned buffer is `\n`.
fn get_aligned_buffer<'a>(file: &File, offset: u64, mut buffer: &'a mut [u8]) -> Result<&'a [u8]> {
    assert!(
        offset == 0 || offset > CHUNK_EXCESS,
        "offset must never be less than chunk excess"
    );
    let metadata = file.metadata()?;
    let file_size = metadata.size();
    if offset > file_size {
        return Ok(&[]);
    }

    let buffer_size = buffer.len().min((file_size - offset) as usize);
    buffer = &mut buffer[..buffer_size];

    let mut head;
    let read_from;

    if offset == 0 {
        head = 0;
        read_from = 0;
    } else {
        head = CHUNK_EXCESS as usize;
        read_from = offset - CHUNK_EXCESS;
    };

    file.read_exact_at(buffer, read_from)?;

    // step backwards until we find the end of the previous record
    // then drop all elements before that
    while head > 0 {
        if buffer[head - 1] == b'\n' {
            break;
        }
        head -= 1;
    }

    // find the end of the final valid record
    let mut tail = buffer.len() - 1;
    while buffer[tail] != b'\n' {
        tail -= 1;
    }

    Ok(&buffer[head..=tail])
}

fn process_chunk(
    file: &File,
    offset: u64,
    outer_map: &mut Arc<Mutex<Map>>,
    buffer: &mut [u8],
) -> Result<()> {
    let aligned_buffer = get_aligned_buffer(file, offset, buffer)?;
    let mut map = BorrowedMap::new();

    for line in aligned_buffer
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let split_point = line
            .iter()
            .enumerate()
            .find_map(|(idx, &b)| (b == b';').then_some(idx))
            .ok_or_else(|| {
                let line = std::str::from_utf8(line).unwrap_or("<invalid utf8>");
                format!("no ';' in {line}")
            })?;

        let temp = std::str::from_utf8(&line[split_point + 1..])
            .map_err(|err| format!("non-utf8 temp: {err}"))?;
        let temp: f32 = temp
            .parse()
            .map_err(|err| format!("parsing {temp}: {err}"))?;

        let city = &line[..split_point];

        map.entry(city)
            .and_modify(|records| records.update(temp))
            .or_insert_with(|| Records::from_item(temp));
    }

    // that should have taken a while; long enough that we can now cheaply update the outer map
    // without worrying too much about contention from other threads
    let mut outer = outer_map.lock().expect("non-poisoned mutex");
    for (city, records) in map.into_iter() {
        let city =
            String::from_utf8(city.to_owned()).map_err(|err| format!("non-utf8 city: {err}"))?;
        outer
            .entry(city)
            .and_modify(|outer_records| *outer_records = outer_records.merge(records))
            .or_insert(records);
    }

    Ok(())
}

fn distribute_work(file: &File) -> Result<Map> {
    let metadata = file.metadata()?;
    let file_size = metadata.size();

    let offset = Arc::new(AtomicU64::new(0));
    let map = Arc::new(Mutex::new(Map::new()));

    thread::scope(|scope| {
        for _ in 0..thread::available_parallelism().map(Into::into).unwrap_or(1) {
            let offset = offset.clone();
            let mut map = map.clone();
            scope.spawn(move || {
                let mut buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
                loop {
                    let offset = offset.fetch_add(CHUNK_SIZE, Ordering::SeqCst);
                    if offset > file_size {
                        break;
                    }

                    process_chunk(file, offset, &mut map, &mut buffer)
                        .expect("processing a chunk should always succeed");
                }
            });
        }
    });

    Ok(Arc::into_inner(map)
        .expect("all other references to map have gone out of scope")
        .into_inner()
        .expect("no poisoned mutexes in this program"))
}

fn run_std() -> Result<()> {
    let file = std::fs::File::open("../measurements.txt")?;
    let map = distribute_work(&file)?;

    let mut keys = map.keys().collect::<Vec<_>>();
    keys.sort_unstable();

    for key in keys {
        let record = map[key];
        let min = record.min;
        let mean = record.mean();
        let max = record.max;

        println!("{key}: {min}/{mean}/{max}");
    }

    Ok(())
}

fn run_polars() -> Result<DataFrame, PolarsError> {
    let now = Instant::now();

    let f1: Field = Field::new("station", DataType::String);
    let f2: Field = Field::new("measure", DataType::Float64);
    let sc: Schema = Schema::from_iter(vec![f1, f2]);

    let q = LazyCsvReader::new("../measurements.txt")
        .has_header(false)
        .with_schema(Some(Arc::new(sc)))
        .with_separator(b';')
        .finish()?
        .group_by(vec![col("station")])
        .agg(vec![
            col("measure").alias("min").min(),
            col("measure").alias("mean").mean(),
            col("measure").alias("max").max(),
        ])
        .sort("station", Default::default())
        .with_streaming(true);

    let df = q.collect()?;

    println!("Time={} μs", now.elapsed().as_secs());

    Ok(df)
}

fn main() -> Result<()> {
    // time
    let now = Instant::now();
    run_std()?;
    println!("Time Rust STD={} seconds", now.elapsed().as_secs());

    // let now = Instant::now();
    // let df = run_polars()?;
    // println!("Time Polars={} seconds", now.elapsed().as_secs());
    // println!("{:?}", df);

    // Crashes Due to much RAM usage
    // purple_mist::main()?;

    rangnargrootkeorkamp::main();

    let now = Instant::now();
    thebracket::read_file()?;
    println!(
        "Time Rust The Bracker Implementation={} seconds",
        now.elapsed().as_secs()
    );

    Ok(())
}
