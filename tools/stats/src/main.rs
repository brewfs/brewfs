//! `brewfs-stats` — Real-time performance monitor for BrewFS.
//!
//! Reads the virtual `.stats` file exposed at the mount root and displays
//! per-second throughput, latency, and ops in a colored terminal UI.
//!
//! Usage:
//!   brewfs-stats /mnt/brewfs          # default 1s interval
//!   brewfs-stats /mnt/brewfs -i 2     # 2s interval

use clap::Parser;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

/// Real-time BrewFS performance stats viewer.
#[derive(Parser, Debug)]
#[command(
    name = "brewfs-stats",
    about = "Monitor BrewFS performance in real-time"
)]
struct Args {
    /// Mount point of BrewFS
    mountpoint: PathBuf,

    /// Refresh interval in seconds
    #[arg(short, long, default_value_t = 1)]
    interval: u64,

    /// Number of header lines before re-printing the header (0 = never)
    #[arg(long, default_value_t = 30)]
    header_every: u32,
}

type Metrics = HashMap<String, u64>;

fn read_stats(path: &PathBuf) -> io::Result<Metrics> {
    let content = std::fs::read_to_string(path)?;
    let mut map = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, val)) = line.split_once(' ')
            && let Ok(v) = val.parse::<u64>()
        {
            map.insert(key.to_string(), v);
        }
    }
    Ok(map)
}

/// Column definition for display.
struct Column {
    label: &'static str,
    width: usize,
    color: Color,
}

/// Section of metrics to display.
struct Section {
    name: &'static str,
    columns: Vec<Column>,
    /// Given (prev, curr, interval_secs), compute values for each column.
    compute: fn(&Metrics, &Metrics, f64) -> Vec<String>,
}

fn format_throughput(bytes_per_sec: f64) -> String {
    if bytes_per_sec >= 1_073_741_824.0 {
        format!("{:.1}G", bytes_per_sec / 1_073_741_824.0)
    } else if bytes_per_sec >= 1_048_576.0 {
        format!("{:.1}M", bytes_per_sec / 1_048_576.0)
    } else if bytes_per_sec >= 1024.0 {
        format!("{:.1}K", bytes_per_sec / 1024.0)
    } else {
        format!("{:.0}", bytes_per_sec)
    }
}

fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M", ops / 1_000_000.0)
    } else if ops >= 1000.0 {
        format!("{:.1}K", ops / 1000.0)
    } else {
        format!("{:.0}", ops)
    }
}

fn format_latency_us(total_us: u64, ops: u64) -> String {
    if ops == 0 {
        return "-".to_string();
    }
    let avg_us = total_us as f64 / ops as f64;
    if avg_us >= 1_000_000.0 {
        format!("{:.1}s", avg_us / 1_000_000.0)
    } else if avg_us >= 1000.0 {
        format!("{:.1}ms", avg_us / 1000.0)
    } else {
        format!("{:.0}us", avg_us)
    }
}

fn delta(prev: &Metrics, curr: &Metrics, key: &str) -> u64 {
    curr.get(key)
        .unwrap_or(&0)
        .saturating_sub(*prev.get(key).unwrap_or(&0))
}

fn build_sections() -> Vec<Section> {
    vec![
        Section {
            name: "fuse",
            columns: vec![
                Column {
                    label: "ops",
                    width: 7,
                    color: Color::Cyan,
                },
                Column {
                    label: "read",
                    width: 8,
                    color: Color::Green,
                },
                Column {
                    label: "write",
                    width: 8,
                    color: Color::Yellow,
                },
                Column {
                    label: "r_lat",
                    width: 7,
                    color: Color::Green,
                },
                Column {
                    label: "w_lat",
                    width: 7,
                    color: Color::Yellow,
                },
            ],
            compute: |prev, curr, dt| {
                let read_ops = delta(prev, curr, "brewfs_fuse_read_ops_total");
                let write_ops = delta(prev, curr, "brewfs_fuse_write_ops_total");
                let lookup_ops = delta(prev, curr, "brewfs_fuse_lookup_ops_total");
                let getattr_ops = delta(prev, curr, "brewfs_fuse_getattr_ops_total");
                let open_ops = delta(prev, curr, "brewfs_fuse_open_ops_total");
                let total_ops = read_ops + write_ops + lookup_ops + getattr_ops + open_ops;

                let read_bytes = delta(prev, curr, "brewfs_fuse_read_bytes_total");
                let write_bytes = delta(prev, curr, "brewfs_fuse_write_bytes_total");

                let read_lat = delta(prev, curr, "brewfs_fuse_read_lat_us_total");
                let write_lat = delta(prev, curr, "brewfs_fuse_write_lat_us_total");

                vec![
                    format_ops(total_ops as f64 / dt),
                    format_throughput(read_bytes as f64 / dt),
                    format_throughput(write_bytes as f64 / dt),
                    format_latency_us(read_lat, read_ops),
                    format_latency_us(write_lat, write_ops),
                ]
            },
        },
        Section {
            name: "meta",
            columns: vec![
                Column {
                    label: "ops",
                    width: 7,
                    color: Color::Magenta,
                },
                Column {
                    label: "txn",
                    width: 6,
                    color: Color::Magenta,
                },
                Column {
                    label: "lat",
                    width: 7,
                    color: Color::Magenta,
                },
            ],
            compute: |prev, curr, dt| {
                let ops = delta(prev, curr, "brewfs_meta_ops_total");
                let txn = delta(prev, curr, "brewfs_meta_txn_ops_total");
                let lat = delta(prev, curr, "brewfs_meta_lat_us_total");
                vec![
                    format_ops(ops as f64 / dt),
                    format_ops(txn as f64 / dt),
                    format_latency_us(lat, ops),
                ]
            },
        },
        Section {
            name: "object",
            columns: vec![
                Column {
                    label: "get",
                    width: 6,
                    color: Color::Blue,
                },
                Column {
                    label: "get/s",
                    width: 8,
                    color: Color::Blue,
                },
                Column {
                    label: "put",
                    width: 6,
                    color: Color::Red,
                },
                Column {
                    label: "put/s",
                    width: 8,
                    color: Color::Red,
                },
                Column {
                    label: "del",
                    width: 5,
                    color: Color::DarkRed,
                },
            ],
            compute: |prev, curr, dt| {
                let get_ops = delta(prev, curr, "brewfs_s3_get_ops_total");
                let get_bytes = delta(prev, curr, "brewfs_s3_get_bytes_total");
                let put_ops = delta(prev, curr, "brewfs_s3_put_ops_total");
                let put_bytes = delta(prev, curr, "brewfs_s3_put_bytes_total");
                let del_ops = delta(prev, curr, "brewfs_s3_del_ops_total");
                vec![
                    format_ops(get_ops as f64 / dt),
                    format_throughput(get_bytes as f64 / dt),
                    format_ops(put_ops as f64 / dt),
                    format_throughput(put_bytes as f64 / dt),
                    format_ops(del_ops as f64 / dt),
                ]
            },
        },
        Section {
            name: "cache",
            columns: vec![
                Column {
                    label: "hit",
                    width: 6,
                    color: Color::Green,
                },
                Column {
                    label: "miss",
                    width: 6,
                    color: Color::Red,
                },
                Column {
                    label: "dirty",
                    width: 8,
                    color: Color::Yellow,
                },
            ],
            compute: |prev, curr, dt| {
                let hits = delta(prev, curr, "brewfs_cache_hits_total");
                let misses = delta(prev, curr, "brewfs_cache_misses_total");
                let dirty = curr.get("brewfs_buffer_dirty_bytes").unwrap_or(&0);
                vec![
                    format_ops(hits as f64 / dt),
                    format_ops(misses as f64 / dt),
                    format_throughput(*dirty as f64),
                ]
            },
        },
    ]
}

fn print_header(sections: &[Section], stdout: &mut io::Stdout) {
    // Section name row
    write!(stdout, "{}", SetAttribute(Attribute::Bold)).unwrap();
    for section in sections {
        let total_width: usize = section.columns.iter().map(|c| c.width + 1).sum::<usize>();
        write!(
            stdout,
            " {:^width$}",
            section.name.to_uppercase(),
            width = total_width
        )
        .unwrap();
        write!(stdout, "│").unwrap();
    }
    writeln!(stdout, "{}", SetAttribute(Attribute::Reset)).unwrap();

    // Column name row
    for section in sections {
        for col in &section.columns {
            write!(
                stdout,
                " {}{:>width$}{}",
                SetForegroundColor(col.color),
                col.label,
                ResetColor,
                width = col.width
            )
            .unwrap();
        }
        write!(stdout, " │").unwrap();
    }
    writeln!(stdout).unwrap();

    // Separator
    for section in sections {
        let total_width: usize = section.columns.iter().map(|c| c.width + 1).sum::<usize>();
        write!(stdout, "{:─>width$}┼", "", width = total_width + 1).unwrap();
    }
    writeln!(stdout).unwrap();
}

fn print_row(
    sections: &[Section],
    prev: &Metrics,
    curr: &Metrics,
    dt: f64,
    stdout: &mut io::Stdout,
) {
    for section in sections {
        let values = (section.compute)(prev, curr, dt);
        for (i, col) in section.columns.iter().enumerate() {
            let val = values.get(i).map(|s| s.as_str()).unwrap_or("-");
            write!(
                stdout,
                " {}{:>width$}{}",
                SetForegroundColor(col.color),
                val,
                ResetColor,
                width = col.width
            )
            .unwrap();
        }
        write!(stdout, " │").unwrap();
    }
    writeln!(stdout).unwrap();
}

fn main() {
    let args = Args::parse();
    let stats_path = args.mountpoint.join(".stats");

    if !stats_path.exists() {
        eprintln!(
            "Error: {} not found.\nMake sure BrewFS is mounted at {:?}",
            stats_path.display(),
            args.mountpoint
        );
        std::process::exit(1);
    }

    let sections = build_sections();
    let mut stdout = io::stdout();
    let interval = Duration::from_secs(args.interval);

    // Read initial snapshot
    let mut prev = match read_stats(&stats_path) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error reading {}: {}", stats_path.display(), e);
            std::process::exit(1);
        }
    };

    let mut line_count: u32 = 0;

    println!(
        "BrewFS Stats — {} (interval: {}s, Ctrl+C to quit)\n",
        args.mountpoint.display(),
        args.interval
    );
    print_header(&sections, &mut stdout);
    stdout.flush().unwrap();

    loop {
        std::thread::sleep(interval);

        let curr = match read_stats(&stats_path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let dt = args.interval as f64;
        print_row(&sections, &prev, &curr, dt, &mut stdout);
        stdout.flush().unwrap();

        line_count += 1;
        if args.header_every > 0 && line_count.is_multiple_of(args.header_every) {
            println!();
            print_header(&sections, &mut stdout);
            stdout.flush().unwrap();
        }

        prev = curr;
    }
}
