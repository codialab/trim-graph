# trim-graph

Simple tool to remove segments/links/jumps from GFA files that are not covered by any paths/walks.

To build:
```bash
cargo build --release
```

To run:
```bash
./target/release/trim-graph ${GFA_FILE}
```

`trim-graph` can also be set to keep only certain paths (+ all their segments/links). For this one needs to have a file containing all the paths that should be kept, one per line.
```bash
./target/release/trim-graph ${GFA_FILE} --paths_to_keep=${PATHS_FILE}
```

While walks are supported by `trim-graph`, it currently has no parameters to only keep certain walks. Thus, to remove a certain group of walks use another tool (e.g. `sed '/W\tHG00741/d'` to remove all walks of sample HG00741) to remove the walks and then run `trim-graph` on the modified graph to trim off the segments/links that are not covered anymore.

Jump lines are also supported and are distinguished from links: if only jumps from segment 11 to segment 12 are used and no links, the links from segment 11 to 12 will be removed (and also vice versa).
