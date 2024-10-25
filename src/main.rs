use clap::Parser;
use itertools::Itertools;
use lazy_static::lazy_static;
use rayon::iter::IntoParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use regex::Regex;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::hash::Hash;
use std::io::Write;

lazy_static! {
    static ref RE: Regex = Regex::new(r"([><])([!-;=?-~]+)").unwrap();
}

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

    /// Do not remove any segment lines
    #[arg(short = 'S', long)]
    ignore_segments: bool,

    /// Do not remove any link lines
    #[arg(short = 'L', long)]
    ignore_links: bool,

    /// Do not remove any jump lines
    #[arg(short = 'J', long)]
    ignore_jumps: bool,
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

type SortedNodes = Vec<String>;
type SortedEdges = Vec<((String, bool), (String, bool))>;
type Nodes = HashSet<String>;
type Edges = HashSet<((String, bool), (String, bool))>;

fn flatten_into_hashset<T: Eq + Hash + Send + Sync + Clone>(v: Vec<Vec<T>>) -> HashSet<T> {
    v.into_par_iter()
        .map(|row| HashSet::from_iter(row.iter().cloned()))
        .reduce(HashSet::new, |acc: HashSet<T>, set| {
            acc.union(&set).cloned().collect()
        })
}

fn get_nodes_edges_from_path(path: &str) -> (SortedNodes, SortedEdges, SortedEdges) {
    let node_texts = path.split_inclusive(&[',', ';']);
    let mut nodes: Vec<(String, bool)> = Vec::new();
    let mut links: SortedEdges = Vec::new();
    let mut jumps: SortedEdges = Vec::new();
    for node_text in node_texts.rev() {
        let node_text = node_text.trim();
        let node = node_text.replace(['+', '-', ',', ';'], "");
        let is_jump = if node_text.ends_with(';') {
            Some(true)
        } else if node_text.ends_with(',') {
            Some(false)
        } else {
            None
        };
        let orientation = if is_jump.is_some() {
            node_text[..node_text.len() - 1].ends_with('+')
        } else {
            node_text[..node_text.len()].ends_with('+')
        };
        println!("{} - {} - {:?}", node, orientation, is_jump);

        if let Some(prev_node) = nodes.last() {
            if is_jump.expect("All nodes before last should have separator") {
                jumps.push(((node.clone(), orientation), prev_node.clone()));
            } else {
                links.push(((node.clone(), orientation), prev_node.clone()));
            }
        }
        nodes.push((node, orientation));
    }
    let nodes = nodes.into_iter().map(|(s, _)| s).collect();
    (nodes, links, jumps)
}

fn get_nodes_edges_from_walk(walk: &str) -> (SortedNodes, SortedEdges) {
    let full_nodes = RE
        .captures_iter(walk)
        .map(|caps| (caps[2].to_string(), &caps[1] == ">"))
        .collect::<Vec<_>>();
    let nodes = full_nodes.iter().cloned().map(|(s, _)| s).collect();
    let links = full_nodes.into_iter().tuple_windows().collect();
    (nodes, links)
}

fn get_nodes_edges(paths: &Vec<String>, walks: &Vec<String>) -> (Nodes, Edges, Edges) {
    let (nodes, (links, jumps)): (Vec<SortedNodes>, (Vec<SortedEdges>, Vec<SortedEdges>)) = paths
        .par_iter()
        .map(|p| {
            let path = p.split('\t').nth(2).unwrap();
            let (nodes, links, jumps) = get_nodes_edges_from_path(path);
            (nodes, (links, jumps))
        })
        .unzip();
    let mut nodes = flatten_into_hashset(nodes);
    let mut links = flatten_into_hashset(links);
    let jumps = flatten_into_hashset(jumps);
    let (walk_nodes, walk_links): (Vec<SortedNodes>, Vec<SortedEdges>) = walks
        .par_iter()
        .map(|w| {
            let w_line = w.split('\t').nth(6).unwrap();
            get_nodes_edges_from_walk(w_line)
        })
        .unzip();
    let walk_nodes = flatten_into_hashset(walk_nodes);
    let walk_links = flatten_into_hashset(walk_links);
    nodes.extend(walk_nodes);
    links.extend(walk_links);
    (nodes, links, jumps)
}

fn filter_segments(segments: Vec<&str>, nodes_to_keep: HashSet<String>) -> Vec<&str> {
    segments
        .into_par_iter()
        .filter(|n| {
            nodes_to_keep.contains(n.split('\t').nth(1).expect("All nodes should have ids"))
        })
        .collect::<Vec<_>>()
}

fn filter_edges(links: Vec<&str>, edges_to_keep: Edges) -> Vec<&str> {
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
    let mut link_lines = Vec::new();
    let mut jump_lines = Vec::new();
    let mut headers = Vec::new();
    let mut others = Vec::new();
    for line in graph {
        if line.starts_with('S') {
            segments.push(line);
        } else if line.starts_with('L') {
            link_lines.push(line);
        } else if line.starts_with('P') {
            paths.push(line);
        } else if line.starts_with('W') {
            walks.push(line);
        } else if line.starts_with('J') {
            jump_lines.push(line);
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

    log::info!("Getting nodes/edges to keep");
    let (nodes, links, jumps) = get_nodes_edges(&paths, &walks);

    let segments = match params.ignore_segments {
        false => {
            log::info!("Removing nodes");
            filter_segments(segments, nodes)
        }
        true => segments,
    };

    let link_lines = match params.ignore_links {
        false => {
            log::info!("Removing links");
            filter_edges(link_lines, links)
        }
        true => link_lines,
    };

    let jump_lines = match params.ignore_jumps {
        false => {
            log::info!("Removing jumps");
            filter_edges(jump_lines, jumps)
        }
        true => jump_lines,
    };

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
    for l in link_lines {
        writeln!(out, "{}", l)?;
    }
    for j in jump_lines {
        writeln!(out, "{}", j)?;
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
    fn test_get_nodes_edges_from_path_nodes() {
        let path = "1+, 2-, 3+";
        let mut expected = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        let (mut calculated, _, _) = get_nodes_edges_from_path(path);
        calculated.sort();
        expected.sort();
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_edges_from_path_links() {
        let path = "1+, 2-; 3+, 2+";
        let mut expected = vec![
            (("1".to_string(), true), ("2".to_string(), false)),
            (("3".to_string(), true), ("2".to_string(), true)),
        ];
        let (_, mut calculated, _) = get_nodes_edges_from_path(path);
        calculated.sort();
        expected.sort();
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_edges_from_path_jumps() {
        let path = "1+; 2-, 3+; 2+";
        let mut expected = vec![
            (("1".to_string(), true), ("2".to_string(), false)),
            (("3".to_string(), true), ("2".to_string(), true)),
        ];
        let (_, _, mut calculated) = get_nodes_edges_from_path(path);
        calculated.sort();
        expected.sort();
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_node_edges_for_paths() {
        let paths = vec!["P\tp1\t1+, 2-; 3+".to_string(), "P\tp2\t2+, 4-".to_string()];
        let expected = (
            HashSet::from([
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string(),
            ]),
            HashSet::from([
                (("1".to_string(), true), ("2".to_string(), false)),
                (("2".to_string(), true), ("4".to_string(), false)),
            ]),
            HashSet::from([(("2".to_string(), false), ("3".to_string(), true))]),
        );
        let calculated = get_nodes_edges(&paths, &Vec::new());
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_edges_from_walk_nodes() {
        let walk = ">1<2>3";
        let mut expected = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        let (mut calculated, _) = get_nodes_edges_from_walk(walk);
        expected.sort();
        calculated.sort();
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_edges_from_walk_links() {
        let walk = ">1<2>3";
        let mut expected = vec![
            (("1".to_string(), true), ("2".to_string(), false)),
            (("2".to_string(), false), ("3".to_string(), true)),
        ];
        let (_, mut calculated) = get_nodes_edges_from_walk(walk);
        expected.sort();
        calculated.sort();
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_nodes_edges_for_walks() {
        let walks = vec![
            "W\tNA12878\t1\tchr1\t0\t11\t>1<2>3".to_string(),
            "W\tNA12878\t1\tchr1\t0\t11\t>2<4".to_string(),
        ];
        let expected = (
            HashSet::from([
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string(),
            ]),
            HashSet::from([
                (("1".to_string(), true), ("2".to_string(), false)),
                (("2".to_string(), false), ("3".to_string(), true)),
                (("2".to_string(), true), ("4".to_string(), false)),
            ]),
            HashSet::from([]),
        );
        let calculated = get_nodes_edges(&Vec::new(), &walks);
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_flatten_into_hashset() {
        let v = vec![vec![1, 2, 3], vec![2, 4]];
        let expected = HashSet::from([1, 2, 3, 4]);
        let calculated = flatten_into_hashset(v);
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
        let calculated = filter_edges(links, links_to_keep);
        assert_eq!(calculated, expected);
    }
}
