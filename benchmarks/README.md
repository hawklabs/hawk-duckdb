# DuckDB Benchmarks for Eclipse Hawk backend

This is a standalone Maven project evaluating the performance of various ways to use DuckDB. To run these benchmarks, use the provided shell scripts:

## Data load benchmarks

`bench-load.sh` runs `LoadBenchmarks`, which compares the relative performance of:

* Single-use prepared statements (with/without autocommit).
* Reused prepared statements (with/without autocommit).
* Generating a CSV file and using the COPY operation.

## Node index mapping benchmarks

`bench-node-index.sh` runs `IndexBenchmarks`, which compares several ways to represent Hawk node indices.
Node indices are conceptually 4-tuples of the form `(index, key, value, node)` that need to support various types of queries:

* Finding all entries in an index: `(index, *, *, *)`.
* Finding all entries in an index with a certain key: `(index, key, *, *)`.
* Finding all nodes in an index with a certain value for a certain key: `(index, key, value, *)`. Note that for a string, the value may be a pattern with globs ("*").
* Finding all entries for a node in an index: `(index, *, *, node)`.
* Finding the entry with a certain key + value for a node: `(index, key, value, node)`.
* Finding all the nodes with values in a key within a certain range: `(index, key, [from, to], *)`.

Values can be floating-point numbers, integers, or strings.
Booleans are typically converted to strings in most Hawk backends.
Null values are silently ignored by the Hawk node index API.
