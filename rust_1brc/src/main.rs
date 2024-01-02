use polars::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let start = Instant::now();
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
    }
    println!("Time: {:?}", start.elapsed());

    let start = Instant::now();
    let df = run_polars().unwrap();
    println!("Time: {:?}", start.elapsed());
    println!("{:?}", df);
}

fn run() -> Result<(), Box<dyn Error>> {
    let file = File::open("measurements.txt")?;
    let reader = BufReader::new(file);

    let mut measurements: HashMap<String, Vec<f64>> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split(';').collect();
        if parts.len() == 2 {
            let station = parts[0].to_string();
            let measure = parts[1].parse::<f64>()?;
            measurements
                .entry(station)
                .or_insert(Vec::new())
                .push(measure);
        }
    }

    for (station, measures) in measurements {
        let min = measures.iter().cloned().fold(f64::INFINITY, f64::min);
        let mean = measures.iter().sum::<f64>() / measures.len() as f64;
        let max = measures.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        println!("{}, {}, {}, {}", station, min, mean, max);
    }

    Ok(())
}

fn run_polars() -> Result<DataFrame, PolarsError> {
    let f1: Field = Field::new("station", DataType::String);
    let f2: Field = Field::new("measure", DataType::Float64);
    let sc: Schema = Schema::from_iter(vec![f1, f2]);

    let q = LazyCsvReader::new("measurements.txt")
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
        .sort("station", Default::default());

    Ok(q.collect()?)
}
