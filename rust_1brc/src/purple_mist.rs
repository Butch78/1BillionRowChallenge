use color_eyre::eyre::{eyre, Result};
use indicatif::{ParallelProgressIterator, ProgressStyle};
use rayon::prelude::*;

const TEMPLATE: &str =
    "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} ETA {eta_precise} ({per_sec})";

#[derive(Debug, Clone, Copy)]
struct WeatherStationStatistics {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
}

impl Default for WeatherStationStatistics {
    fn default() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            count: 0,
        }
    }
}

pub fn main() -> Result<()> {
    color_eyre::install()?;

    let map = std::fs::read_to_string("../measurements.txt")?
        .lines()
        .collect::<Vec<_>>()
        .into_par_iter()
        .progress_with_style(ProgressStyle::default_bar().template(TEMPLATE)?)
        .map(|line| -> Result<_> {
            let (id, measurement) = line
                .split_once(';')
                .ok_or_else(|| eyre!("invalid line {line:?}"))?;
            Ok((id, measurement.parse::<f64>()?))
        })
        .try_fold_with(
            rustc_hash::FxHashMap::<String, WeatherStationStatistics>::default(),
            |mut acc, res| -> Result<_> {
                let (id, measurement) = res?;
                let stats = acc.entry(id.to_string()).or_default();
                stats.min = stats.min.min(measurement);
                stats.max = stats.max.max(measurement);
                stats.sum += measurement;
                stats.count += 1;
                Ok(acc)
            },
        )
        .try_reduce(
            || rustc_hash::FxHashMap::<String, WeatherStationStatistics>::default(),
            |mut acc, map| -> Result<_> {
                for (id, stats) in map {
                    let acc_stats = acc.entry(id).or_default();
                    acc_stats.min = acc_stats.min.min(stats.min);
                    acc_stats.max = acc_stats.max.max(stats.max);
                    acc_stats.sum += stats.sum;
                    acc_stats.count += stats.count;
                }
                Ok(acc)
            },
        )?;

    for (id, stats) in map {
        println!(
            "{}={}/{}/{}",
            id,
            stats.min,
            stats.max,
            stats.sum / stats.count as f64
        );
    }

    Ok(())
}
