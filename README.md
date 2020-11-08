# Experimental DuckDB backend for Eclipse Hawk

This is a WIP experimental backend for Eclipse Hawk, using the [DuckDB](https://www.eclipse.org/hawk/) embedded OLAP database.
It is not fit for use yet, as we are still figuring out what's the best mapping from the property graph model in Hawk.

This repository contains the following folders:

* `plugins`: Eclipse plug-in projects that implement the new backend. You will need to have a copy of the [Eclipse Hawk](https://www.eclipse.org/hawk/developers/run-from-source/) source code in your Eclipse IDE before you can work on them.
* `benchmarks`: standalone Maven project which evaluates the relative performance of various ways of working with DuckDB. You can work on this project without the rest of the code of Hawk. Please check its [README.md](benchmarks/README.md) for details.
