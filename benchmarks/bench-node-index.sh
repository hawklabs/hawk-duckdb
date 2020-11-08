#!/bin/sh

mvn compile exec:java -Dexec.mainClass=org.eclipse.hawk.duckdb.benchmarks.index.IndexBenchmarks  
