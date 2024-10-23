use clap::Parser;
use itertools::Itertools;
use log;
use rayon::iter::IntoParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io::Write;

#[derive(Parser)]
#[command(version, about)]
struct Params {
    /// Graph that should be trimmed
    graph_file: String,

    /// File containing a list of paths to keep, if this is not set all paths are kept
    #[arg(short, long, value_name = "FILE")]
    paths_to_keep: Option<String>,

    /// Sets the number of threads for trim-graph to use
    #[arg(short, long)]
    threads: Option<usize>,
}

fn set_number_of_threads(params: &Params) {
    let threads = params.threads.unwrap_or(4);
    //if num_threads is 0 then the Rayon will select
    //the number of threads to the core number automatically
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .expect("Failed to initialize global thread pool");
    log::info!(
        "running trim-graph on {} threads",
        rayon::current_num_threads()
    );
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let params = Params::parse();

    set_number_of_threads(&params);

    let graph_content =
        fs::read_to_string(params.graph_file).expect("Should have been able to read the file");
    let graph = graph_content.lines().collect::<Vec<_>>();

    let mut segments = Vec::new();
    let mut paths = Vec::new();
    let mut links = Vec::new();
    let mut headers = Vec::new();
    for line in graph {
        if line.starts_with("S") {
            segments.push(line);
        } else if line.starts_with("L") {
            links.push(line);
        } else if line.starts_with("P") {
            paths.push(line);
        } else if line.starts_with("H") {
            headers.push(line);
        }
    }

    let paths_to_keep = match params.paths_to_keep {
        Some(path_file) => {
            let contents =
                fs::read_to_string(path_file).expect("Should have been able to read the file");
            contents.lines().map(|s| s.to_string()).collect::<Vec<_>>()
        }
        None => paths
            .par_iter()
            .map(|l| {
                l.split("\t")
                    .skip(1)
                    .next()
                    .expect("All paths should have names")
                    .to_string()
            })
            .collect(),
    };

    log::info!("Filtering paths");
    let paths = paths
        .into_par_iter()
        .filter(|l| {
            paths_to_keep.contains(
                &l.split("\t")
                    .skip(1)
                    .next()
                    .expect("All paths should have names")
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();

    log::info!("Getting nodes to keep");
    let nodes_of_paths = paths
        .par_iter()
        .map(|p| {
            p.split("\t")
                .skip(2)
                .next()
                .unwrap()
                .split(",")
                .map(|s| {
                    let trimmed = s.trim();
                    trimmed[..trimmed.len() - 1].to_string()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let nodes_to_keep = nodes_of_paths
        .par_iter()
        .map(|nodes| HashSet::from_iter(nodes.clone()))
        .reduce(
            || HashSet::new(),
            |acc: HashSet<String>, set| acc.union(&set).map(|s| s.to_string()).collect(),
        );

    log::info!("Removing nodes");
    let segments = segments
        .into_par_iter()
        .filter(|n| {
            nodes_to_keep.contains(
                n.split("\t")
                    .skip(1)
                    .next()
                    .expect("All nodes should have ids"),
            )
        })
        .collect::<Vec<_>>();

    log::info!("Getting edges to keep");
    let edges_to_keep = nodes_of_paths
        .into_par_iter()
        .map(|p| p.into_iter().tuple_windows().collect::<HashSet<(_, _)>>())
        .reduce(
            || HashSet::new(),
            |acc, set| {
                acc.union(&set)
                    .map(|s| (s.0.to_string(), s.1.to_string()))
                    .collect()
            },
        );

    log::info!("Removing edges");
    let links = links
        .into_par_iter()
        .filter(|l| {
            let fields = l.split("\t").collect::<Vec<_>>();
            let edge = (fields[1].to_string(), fields[3].to_string());
            let rev_edge = (fields[3].to_string(), fields[1].to_string());
            edges_to_keep.contains(&edge) || edges_to_keep.contains(&rev_edge)
        })
        .collect::<Vec<_>>();

    let mut out = std::io::BufWriter::new(std::io::stdout());
    for h in headers {
        writeln!(out, "{}", h)?;
    }
    for s in segments {
        writeln!(out, "{}", s)?;
    }
    for p in paths {
        writeln!(out, "{}", p)?;
    }
    for l in links {
        writeln!(out, "{}", l)?;
    }
    Ok(())
}
