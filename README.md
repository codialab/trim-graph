# trim-graph

Simple tool to remove segments/links from GFA files that are not covered by any paths.

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
