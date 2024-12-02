<br>

<p align="center">
    <img width=120 src="https://raw.githubusercontent.com/surrealdb/icons/main/surreal.svg" />
</p>

<h3 align="center">A collection of benchmarking tools and libraries for testing, <br> comparing, and improving the performance of SurrealDB.</h3>

<br>

<p align="center">
    <a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6"></a>
    &nbsp;
    <a href="https://twitter.com/surrealdb"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.youtube.com/channel/UCjf2teVEuYVvvVC-gFZNq6w"><img src="https://img.shields.io/badge/youtube-subscribe-fc1c1c.svg?style=flat-square"></a>
</p>

<p align="center">
    <a href="https://surrealdb.com/blog"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/blog.svg" alt="Blog"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/surrealdb"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/github.svg" alt="Github	"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/linkedin.svg" alt="LinkedIn"></a>
    &nbsp;
    <a href="https://twitter.com/surrealdb"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/twitter.svg" alt="Twitter"></a>
    &nbsp;
    <a href="https://www.youtube.com/channel/UCjf2teVEuYVvvVC-gFZNq6w"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/youtube.svg" alt="Youtube"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/dev.svg" alt="Dev"></a>
    &nbsp;
    <a href="https://surrealdb.com/discord"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/discord.svg" alt="Discord"></a>
    &nbsp;
    <a href="https://stackoverflow.com/questions/tagged/surrealdb"><img height="25" src="https://raw.githubusercontent.com/surrealdb/.github/main/img/social/stack-overflow.svg" alt="StackOverflow"></a>
</p>

<br>

# Benchmarks

This repository exists as a central repository for a collection of benchmarking tools and libraries for testing, comparing, and improving the performance of SurrealDB. Each tool linked to from this location exists within its own repository.

As we add support for additional benchmarking suites, we will add and link to them in this repository.

## [crud-bench](https://github.com/surrealdb/crud-bench)

> [!TIP]
> The `crud-bench` tool is being actively developed. Please ensure that you are running the latest code.

The crud-bench benchmarking tool is an open-source benchmarking tool for testing and comparing the performance of a number of different workloads on embedded, networked, and remote databases. It can be used to compare both SQL and NoSQL platforms including key-value, and embedded databases. Importantly crud-bench focuses on testing additional features which are not present in other benchmarking tools, but which are available in SurrealDB.

The primary purpose of crud-bench is to continually test and monitor the performance of features and functionality built in to SurrealDB, enabling developers working on features in SurrealDB to assess the impact of their changes on database queries and performance.

The crud-bench benchmarking tool is being actively developed with new features and functionality being added regularly.

<details>

<summary>Getting started with the crud-bench benchmarking tool</summary>

<br>

1. Navigate to the crud-bench [repository](https://github.com/surrealdb/crud-bench).
```sh
git clone -b main https://github.com/surrealdb/crud-bench
```
2. Build the `crud-bench` tool in release mode
```sh
cargo build --release && ln -s target/release/crud-bench crud-bench
```
3. View the configuration options
```sh
crud-bench --help
```
4. Ensure the Docker daemon is running
```sh
dockerd
```
5. Run a local benchmark with custom options
```sh
crud-bench -d surrealdb-rocksdb -s 5000000 -c 128 -t 48 -k string26 -r
```
6. Run a remote benchmark against a remote SurrealDB server
```sh
crud-bench -d surrealdb --endpoint ws://127.0.0.1:8000 -s 5000000 -c 128 -t 48 -k string26 -r
```
</details>

## [ann-benchmarks](https://github.com/surrealdb/ann-benchmarks/tree/surrealdb)

The Approximate Nearest Neighbour benchmarks suite is a testing tool for comparing the performance of vector datasets, with a focus on approximate and exact nearest neighbour algorithms across different data platforms, databases, and libraries. This project contains tools to benchmark various implementations of approximate nearest neighbor, and exact nearest neighbour search for selected metrics - with pre-generated datasets, and a test suite to verify function integrity.

<details>

<summary>Getting started with the ann-benchmarks benchmarking tool</summary>

<br>

1. Navigate to the `surrealdb` [branch](https://github.com/surrealdb/ann-benchmarks/tree/surrealdb) on the repository.
```sh
git clone -b surrealdb https://github.com/surrealdb/ann-benchmarks
```
2. Setup a Python virtual environment
```sh
python3.10 -m venv path/to/venv && source path/to/venv/bin/activate
```
3. Install the benchmark requirements
```sh
pip3.10 install -r requirements.txt
```
4. Install the desired benchmark algorithm
```sh
python3.10 install.py --algorithm surreal_hnsw
```
5. Run a specific benchmark with a specified dataset
```sh
python3.10 run.py --run-disabled --algorithm surreal_hnsw --dataset random-xs-20-euclidean
```
</details>

## [go-ycsb](https://github.com/surrealdb/go-ycsb/tree/surrealdb)

The Yahoo! Cloud Serving Benchmark (YCSB) is an open-source specification and program suite for evaluating retrieval and maintenance capabilities of computer programs. It is often used to compare the relative performance of NoSQL database management systems. This Golang port of the original benchmarking suite allows benchmarking comparison using the Golang programing language.

<details>

<summary>Getting started with the go-ycsb benchmarking tool</summary>

<br>

1. Navigate to the `surrealdb` [branch](https://github.com/surrealdb/go-ycsb/tree/surrealdb) on the repository.
```sh
git clone -b surrealdb https://github.com/surrealdb/go-ycsb
```
2. Build the go-ycsb binary
```sh
make quick
```
3. Test a workload locally against SurrealDB
```sh
./bin/go-ycsb load surrealdb -P workloads/workloada
./bin/go-ycsb run surrealdb -P workloads/workloada
```
4. Test a workload against a remote SurrealDB server
```sh
./bin/go-ycsb load surrealdb -P workloads/workloada -p surrealdb.uri='ws://127.0.0.1:8000' -p surrealdb.user=root -p surrealdb.pass=root
./bin/go-ycsb run surrealdb -P workloads/workloada -p surrealdb.uri='ws://127.0.0.1:8000' -p surrealdb.user=root -p surrealdb.pass=root
```
</details>

## [nosqlbench](https://github.com/surrealdb/nosqlbench/tree/surrealdb)

> [!IMPORTANT]
> This SurrealDB changes to this benchmarking tool have not yet been released. Please check back later.

NoSQLBench is a serious performance testing tool for the NoSQL ecosystem, sponsored by DataStax. It brings together features and capabilities that are not found in any other tool. The core machinery of NoSQLBench has been built with attention to detail. It has been battle tested within DataStax and in the NoSQL ecosystem as a way to help users validate their data models, baseline system performance, and qualify system designs for scale.
