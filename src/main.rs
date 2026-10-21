use anyhow::anyhow;
use clap::Parser;
use env_logger::Env;
use itertools::Itertools;
use lazy_static::lazy_static;
use rayon::iter::IntoParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use regex::regex;
use regex::Regex;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{self, File};
use std::hash::Hash;
use std::io::{BufRead, BufReader, Write};

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

    /// Induces link lines from the paths
    #[arg(short = 'i', long)]
    induce: bool,
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

fn get_paths(paths: Vec<&str>, paths_to_keep: &Option<Vec<SpecifiedWalk>>) -> Vec<String> {
    if let Some(paths_to_keep) = paths_to_keep.as_ref() {
        log::info!("Filtering paths");
        let paths = paths
            .into_par_iter()
            .filter_map(|l| {
                let walk = Walk::try_from_path(l).ok()?;
                if paths_to_keep.iter().any(|s| walk.matches(s)) {
                    Some(l.to_owned())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        paths
    } else {
        log::info!("No path filtering done");
        paths.into_iter().map(|s| s.to_owned()).collect()
    }
}

fn get_walks(walks: Vec<&str>, walks_to_keep: &Option<Vec<SpecifiedWalk>>) -> Vec<String> {
    if let Some(walks_to_keep) = walks_to_keep.as_ref() {
        log::info!("Filtering walks");
        let walks = walks
            .into_par_iter()
            .filter_map(|l| {
                let walk = Walk::try_from_walk(l).ok()?;
                if walks_to_keep.iter().any(|s| walk.matches(s)) {
                    Some(l.to_owned())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        walks
    } else {
        log::info!("No path filtering done");
        walks.into_iter().map(|s| s.to_owned()).collect()
    }
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
        //eprintln!("{} - {} - {:?}", node, orientation, is_jump);

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

fn filter_edges_string(links: Vec<String>, edges_to_keep: Edges) -> Vec<String> {
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

fn induce_edges(links: Vec<&str>, edges_to_keep: Edges) -> Vec<String> {
    let link_check: HashSet<&str> = links.iter().cloned().collect();
    let edges: Vec<String> = edges_to_keep
        .into_iter()
        .filter_map(|e| {
            let text = format!(
                "L\t{}\t{}\t{}\t{}\t0M",
                e.0 .0,
                bto(e.0 .1),
                e.1 .0,
                bto(e.1 .1)
            );
            if link_check.contains(&text[..]) {
                None
            } else {
                Some(text)
            }
        })
        .collect();
    let mut links: Vec<String> = links.into_iter().map(|s| s.to_string()).collect();
    eprintln!("Found Edges: {:?}", edges);
    links.extend(edges);
    links
}

fn bto(o: bool) -> String {
    match o {
        true => '+'.to_string(),
        false => '-'.to_string(),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

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

    let paths_to_keep: Option<anyhow::Result<Vec<SpecifiedWalk>>> =
        params.paths_to_keep.map(|file_name| {
            let file = File::open(file_name)?;
            let reader = BufReader::new(file);
            let mut paths: Vec<SpecifiedWalk> = Vec::new();
            for line in reader.lines() {
                paths.push((&line?[..]).try_into()?);
            }
            Ok(paths)
        });
    let paths_to_keep = paths_to_keep.map_or(Ok(None), |v| v.map(Some))?;

    let paths = get_paths(paths, &paths_to_keep);
    let walks = get_walks(walks, &paths_to_keep);

    log::info!("Getting nodes/edges to keep");
    let (nodes, links, jumps) = get_nodes_edges(&paths, &walks);

    let segments = match params.ignore_segments {
        false => {
            log::info!("Removing nodes");
            filter_segments(segments, nodes)
        }
        true => segments,
    };

    let link_lines = match params.induce {
        false => link_lines.into_iter().map(|s| s.to_string()).collect(),
        true => {
            log::info!("Inducing links");
            induce_edges(link_lines, links.clone())
        }
    };

    let link_lines = match params.ignore_links {
        false => {
            log::info!("Removing links");
            filter_edges_string(link_lines, links)
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

#[derive(Debug)]
enum SpecifiedWalk {
    SampleLevel(String),
    HaplotypeLevel(String, u16),
    SequenceLevel(String, u16, String),
    CoordLevel(String, u16, String, usize, usize),
}

#[derive(Debug)]
struct Walk {
    sample: String,
    haplotype: u16,
    sequence: String,
    start: Option<usize>,
    end: Option<usize>,
}

impl Walk {
    fn try_from_path(path: &str) -> anyhow::Result<Self> {
        let caps = regex!(r"^P\t([^#]+)#(\d+)#([^#:]+)(:(\d+)-(\d+))?\t")
            .captures(path)
            .ok_or(anyhow!("Path line is not a valid path line"))?;
        let sample = caps[1].to_owned();
        let haplotype: u16 = caps[2].parse()?;
        let sequence = caps[3].to_owned();
        let start: anyhow::Result<Option<usize>> = caps
            .get(5)
            .map_or(Ok(None), |v| Ok(Some(v.as_str().parse()?)));
        let start = start?;
        let end: anyhow::Result<Option<usize>> = caps
            .get(6)
            .map_or(Ok(None), |v| Ok(Some(v.as_str().parse()?)));
        let end = end?;
        Ok(Self {
            sample,
            haplotype,
            sequence,
            start,
            end,
        })
    }

    fn try_from_walk(walk: &str) -> anyhow::Result<Self> {
        let caps = regex!(r"^W\t([^\t]+)\t(\d+)\t([^\t]+)\t([0-9*]+)\t([0-9*]+)\t")
            .captures(walk)
            .ok_or(anyhow!("Walk line is not a valid walk line"))?;
        let sample = caps[1].to_owned();
        let haplotype: u16 = caps[2].parse()?;
        let sequence = caps[3].to_owned();
        let start: anyhow::Result<Option<usize>> = caps.get(4).map_or(Ok(None), |v| {
            let v = v.as_str();
            if v == "*" {
                Ok(None)
            } else {
                Ok(Some(v.parse()?))
            }
        });
        let start = start?;
        let end: anyhow::Result<Option<usize>> = caps.get(5).map_or(Ok(None), |v| {
            let v = v.as_str();
            if v == "*" {
                Ok(None)
            } else {
                Ok(Some(v.parse()?))
            }
        });
        let end = end?;
        Ok(Self {
            sample,
            haplotype,
            sequence,
            start,
            end,
        })
    }

    fn matches(&self, specified: &SpecifiedWalk) -> bool {
        match specified {
            SpecifiedWalk::SampleLevel(sample) => &self.sample == sample,
            SpecifiedWalk::HaplotypeLevel(sample, haplotype) => {
                &self.sample == sample && &self.haplotype == haplotype
            }
            SpecifiedWalk::SequenceLevel(sample, haplotype, sequence) => {
                &self.sample == sample && &self.haplotype == haplotype && &self.sequence == sequence
            }
            SpecifiedWalk::CoordLevel(sample, haplotype, sequence, start, end) => {
                if let (Some(self_start), Some(self_end)) = (self.start.as_ref(), self.end.as_ref())
                {
                    &self.sample == sample
                        && &self.haplotype == haplotype
                        && &self.sequence == sequence
                        && self_start == start
                        && self_end == end
                } else {
                    false
                }
            }
        }
    }
}

impl TryFrom<&str> for SpecifiedWalk {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> anyhow::Result<Self> {
        let fields: Vec<&str> = value.trim().split_whitespace().collect();
        match fields.len() {
            1 => Ok(Self::SampleLevel(fields[0].to_owned())),
            2 => Ok(Self::HaplotypeLevel(
                fields[0].to_owned(),
                fields[1].parse()?,
            )),
            3 => Ok(Self::SequenceLevel(
                fields[0].to_owned(),
                fields[1].parse()?,
                fields[2].to_owned(),
            )),
            5 => Ok(Self::CoordLevel(
                fields[0].to_owned(),
                fields[1].parse()?,
                fields[2].to_owned(),
                fields[3].parse()?,
                fields[4].parse()?,
            )),
            _ => Err(anyhow!("Line {} does not follow path structure", value)),
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_get_paths() {
        let paths = vec![
            "P\tp1#0#a\t1+, 2-, 3+",
            "P\tp2#0#a\t2+, 4-",
            "P\tp3#0#a\t5-, 3-, 1+",
        ];
        let paths_to_keep = Some(vec![
            SpecifiedWalk::SampleLevel("p2".to_string()),
            SpecifiedWalk::SampleLevel("p3".to_string()),
        ]);
        let calculated = get_paths(paths, &paths_to_keep);
        let expected = vec![
            "P\tp2#0#a\t2+, 4-".to_string(),
            "P\tp3#0#a\t5-, 3-, 1+".to_string(),
        ];
        assert_eq!(calculated, expected);
    }

    #[test]
    fn test_get_walks() {
        let paths = vec![
            "W\tp1\t0\ta\t*\t*\t1+, 2-, 3+",
            "W\tp2\t0\ta\t*\t*\t2+, 4-",
            "W\tp3\t0\ta\t*\t*\t5-, 3-, 1+",
        ];
        let paths_to_keep = Some(vec![
            SpecifiedWalk::SampleLevel("p2".to_string()),
            SpecifiedWalk::SampleLevel("p3".to_string()),
        ]);
        let calculated = get_walks(paths, &paths_to_keep);
        let expected = vec![
            "W\tp2\t0\ta\t*\t*\t2+, 4-".to_string(),
            "W\tp3\t0\ta\t*\t*\t5-, 3-, 1+".to_string(),
        ];
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
