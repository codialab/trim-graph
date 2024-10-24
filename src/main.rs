use clap::Parser;
use itertools::Itertools;
use rayon::iter::IntoParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io::Write;
use regex::Regex;

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

fn get_paths(paths: Vec<&str>, paths_to_keep: Vec<String>) -> Vec<String> {
    log::info!("Filtering paths");
    let paths = paths
        .into_par_iter()
        .filter(|l| {
            paths_to_keep.contains(
                &l.split('\t')
                    .nth(1)
                    .expect("All paths should have names")
                    .to_string(),
            )
        })
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    paths
}

fn get_nodes_of_paths_walks(paths: &Vec<String>, walks: &Vec<String>) -> Vec<Vec<(String, bool)>> {
    let regex = Regex::new(r"([><])([!-;=?-~]+)").unwrap(); 
    let mut paths_walks = paths
        .par_iter()
        .map(|p| {
            p.split('\t')
                .nth(2)
                .unwrap()
                .split(',')
                .map(|s| {
                    let trimmed = s.trim();
                    (
                        trimmed[..trimmed.len() - 1].to_string(),
                        trimmed.ends_with('+'),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    paths_walks.append(&mut walks
        .par_iter()
        .map(|w| {
            let w_line = w.split('\t').nth(6).unwrap();
            regex.captures_iter(w_line).map(|caps| (caps[2].to_string(), &caps[1] == ">")).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>());
    paths_walks
}

fn get_segments_to_keep(nodes_of_paths: &Vec<Vec<(String, bool)>>) -> HashSet<String> {
    nodes_of_paths
        .par_iter()
        .map(|nodes| {
            HashSet::from_iter::<HashSet<String>>(
                nodes
                    .clone()
                    .into_iter()
                    .unzip::<String, bool, HashSet<String>, HashSet<bool>>()
                    .0,
            )
        })
        .reduce(HashSet::new, |acc: HashSet<String>, set| {
            acc.union(&set).map(|s| s.to_string()).collect()
        })
}

fn filter_segments(segments: Vec<&str>, nodes_to_keep: HashSet<String>) -> Vec<&str> {
    segments
        .into_par_iter()
        .filter(|n| {
            nodes_to_keep.contains(n.split('\t').nth(1).expect("All nodes should have ids"))
        })
        .collect::<Vec<_>>()
}

fn get_links_to_keep(
    nodes_of_paths: Vec<Vec<(String, bool)>>,
) -> HashSet<((String, bool), (String, bool))> {
    nodes_of_paths
        .into_par_iter()
        .map(|p| p.into_iter().tuple_windows().collect::<HashSet<(_, _)>>())
        .reduce(HashSet::new, |acc, set| {
            acc.union(&set)
                .map(|s| (s.0.to_owned(), s.1.to_owned()))
                .collect()
        })
}

fn filter_links(
    links: Vec<&str>,
    edges_to_keep: HashSet<((String, bool), (String, bool))>,
) -> Vec<&str> {
    links
        .into_par_iter()
        .filter(|l| {
            let fields = l.split('\t').collect::<Vec<_>>();
            let edge = (
                (fields[1].to_string(), fields[2].contains('+')),
                (fields[3].to_string(), fields[4].contains('+')),
            );
            let rev_edge = (
                (fields[3].to_string(), fields[4].contains('+')),
                (fields[1].to_string(), fields[2].contains('+')),
            );
            edges_to_keep.contains(&edge) || edges_to_keep.contains(&rev_edge)
        })
        .collect::<Vec<_>>()
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
    let mut walks = Vec::new();
    let mut links = Vec::new();
    let mut headers = Vec::new();
    let mut others = Vec::new();
    for line in graph {
        if line.starts_with('S') {
            segments.push(line);
        } else if line.starts_with('L') {
            links.push(line);
        } else if line.starts_with('P') {
            paths.push(line);
        } else if line.starts_with('W'){
            walks.push(line);
        } else if line.starts_with('H') {
            headers.push(line);
        } else {
            others.push(line);
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
                l.split('\t')
                    .nth(1)
                    .expect("All paths should have names")
                    .to_string()
            })
            .collect(),
    };

    let paths = get_paths(paths, paths_to_keep);
    let walks = walks.into_par_iter().map(|s| s.to_string()).collect();

    log::info!("Getting nodes to keep");
    let nodes_of_paths_walks = get_nodes_of_paths_walks(&paths, &walks);
    let nodes_to_keep = get_segments_to_keep(&nodes_of_paths_walks);

    log::info!("Removing nodes");
    let segments = filter_segments(segments, nodes_to_keep);

    log::info!("Getting edges to keep");
    let edges_to_keep = get_links_to_keep(nodes_of_paths_walks);

    log::info!("Removing edges");
    let links = filter_links(links, edges_to_keep);

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
    for w in walks {
        writeln!(out, "{}", w)?;
    }
    for l in links {
        writeln!(out, "{}", l)?;
    }
    for o in others {
        writeln!(out, "{}", o)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_get_paths() {
        let paths = vec!["P\tp1\t1+, 2-, 3+", "P\tp2\t2+, 4-", "P\tp3\t5-, 3-, 1+"];
        let paths_to_keep = vec!["p2".to_string(), "p3".to_string()];
        let calculated = get_paths(paths, paths_to_keep);
        let expected = vec!["P\tp2\t2+, 4-".to_string(), "P\tp3\t5-, 3-, 1+".to_string()];
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_of_paths() {
        let paths = vec![
            "P\tp1\t1+, 2-, 3+".to_string(),
            "P\tp2\t2+, 4-".to_string(),
            "P\tp3\t5-, 3-, 1+".to_string(),
        ];
        let expected = vec![
            vec![
                ("1".to_string(), true),
                ("2".to_string(), false),
                ("3".to_string(), true),
            ],
            vec![("2".to_string(), true), ("4".to_string(), false)],
            vec![
                ("5".to_string(), false),
                ("3".to_string(), false),
                ("1".to_string(), true),
            ],
        ];
        let calculated = get_nodes_of_paths_walks(&paths, &Vec::new());
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_of_walks() {
        let walks = vec![
            "W\tNA12878\t1\tchr1\t0\t11\t>s11<s12>s13".to_string(),
            "W\tNA12878\t1\tchr1\t0\t11\t<s112<s12s>s13<s11".to_string(),
        ];
        let expected = vec![
            vec![
                ("s11".to_string(), true),
                ("s12".to_string(), false),
                ("s13".to_string(), true),
            ],
            vec![
                ("s112".to_string(), false),
                ("s12s".to_string(), false),
                ("s13".to_string(), true),
                ("s11".to_string(), false),
            ],
        ];
        let calculated = get_nodes_of_paths_walks(&Vec::new(), &walks);
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_segments_to_keep() {
        let nodes_of_paths = vec![
            vec![
                ("1".to_string(), true),
                ("2".to_string(), false),
                ("3".to_string(), true),
            ],
            vec![("2".to_string(), true), ("4".to_string(), false)],
            vec![
                ("5".to_string(), false),
                ("3".to_string(), false),
                ("1".to_string(), true),
            ],
        ];
        let expected = HashSet::from([
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
            "5".to_string(),
        ]);
        let calculated = get_segments_to_keep(&nodes_of_paths);
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_filter_segments() {
        let segments = vec!["S\t1\tTCCGAT", "S\t2\tTA", "S\t3\tACG"];
        let nodes = HashSet::from(["1".to_string(), "2".to_string()]);
        let expected = vec!["S\t1\tTCCGAT", "S\t2\tTA"];
        let calculated = filter_segments(segments, nodes);
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_links_to_keep() {
        let nodes_of_paths = vec![
            vec![
                ("1".to_string(), true),
                ("2".to_string(), false),
                ("3".to_string(), true),
            ],
            vec![("2".to_string(), true), ("4".to_string(), false)],
            vec![
                ("5".to_string(), false),
                ("3".to_string(), false),
                ("1".to_string(), true),
            ],
        ];
        let expected = HashSet::from([
            (("1".to_string(), true), ("2".to_string(), false)),
            (("2".to_string(), false), ("3".to_string(), true)),
            (("2".to_string(), true), ("4".to_string(), false)),
            (("5".to_string(), false), ("3".to_string(), false)),
            (("3".to_string(), false), ("1".to_string(), true)),
        ]);
        let calculated = get_links_to_keep(nodes_of_paths);
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_filter_links() {
        let links = vec![
            "L\t2\t-\t1\t+",
            "L\t2\t-\t3\t+",
            "L\t2\t-\t4\t+",
            "L\t5\t-\t4\t+",
        ];
        let links_to_keep = HashSet::from([
            (("1".to_string(), true), ("2".to_string(), false)),
            (("2".to_string(), false), ("3".to_string(), true)),
            (("2".to_string(), true), ("4".to_string(), false)),
            (("5".to_string(), false), ("3".to_string(), false)),
        ]);
        let expected = vec!["L\t2\t-\t1\t+", "L\t2\t-\t3\t+"];
        let calculated = filter_links(links, links_to_keep);
        assert_eq!(calculated, expected);
    }
}
