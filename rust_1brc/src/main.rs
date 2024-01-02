use polars::prelude::*;
/// Implementation for the 1 Billion Row Challenge, set here: https://www.morling.dev/blog/one-billion-row-challenge/
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::time::Instant;

struct Aggregate {
    min: f64,
    max: f64,
    mean: f64,
    sum: f64,
    measurements: usize,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean, self.max)
    }
}

fn run_std() {
    let now = Instant::now();

    let f = File::open("../measurements.txt").unwrap();
    let reader = BufReader::new(f);

    let mut res_map = BTreeMap::<String, Aggregate>::new();
    for line in reader.lines() {
        if let Some((name_str, measurement_str)) = line.unwrap().split_once(";") {
            let name_string = name_str.to_string();
            let measurement = f64::from_str(measurement_str.trim()).unwrap();
            if let Some(aggr) = res_map.get_mut(&name_string) {
                if measurement.lt(&aggr.min) {
                    aggr.min = measurement;
                }
                if measurement.gt(&aggr.min) {
                    aggr.max = measurement;
                }
                // Note: for performance, we calculate the mean at the end
                aggr.sum += measurement;
                aggr.measurements += 1;
            } else {
                res_map.insert(
                    name_string,
                    Aggregate {
                        min: measurement,
                        max: measurement,
                        mean: measurement,
                        sum: measurement,
                        measurements: 1,
                    },
                );
            }
        }
    }

    for aggr in res_map.values_mut() {
        aggr.mean = aggr.sum / (aggr.measurements as f64)
    }

    for (name, aggr) in res_map {
        println!("{}={}", name, aggr)
    }

    println!("Time={} μs", now.elapsed().as_micros())
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

fn main() {
    run_polars();
    // run_std();
}
