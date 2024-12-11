# Performance

To achieve the best performance from SurrealDB, there are a number of configuration options and runtime design choices to be considered which can improve and affect the performance of the database.

This document aims to guide users who want to run SurrealDB in production environments, or who are looking to get the best performance out of the database. We will look at configuration options when starting SurrealDB, design choices when storing data in SurrealDB, the performance characteristics of certain queries within SurrealQL, in addition to what is and isn't yet supported, and what we are working on next with regards to performance improvements and optimisations.

## Overview

Before we dive in to what SurrealDB is, it's important to note what SurrealDB is not. Although SurrealDB can be used in many different ways, and to store and query data with many different models, at its core, SurrealDB is a transactional document database.

SurrealDB is at its core, a transactional document database. When data is inserted into SurrealDB, data is stored in documents, in a similar way to other document databases.

One of the fundamental architecture designs of SurrealDB is that the storage layer is separated from the computation layer. This means that the layer that is used to store and persist any data, is kept separate from the layer which parses the SurrealQL queries, processes the  primary desgn choices of SurrealDB is that
- SurrealDB is a transactional, document database, not columnar

<br>

## Running SurrealDB

### Running SurrealDB as a server

When starting the SurrealDB server, it is important to run the server using the correct configuration options and settings. For production environments or for performance benchmarking, the `--log` command-line argument or the `SURREAL_LOG` environment variable should be set to `error`, `warn`, or `info` (the default option when not specified). Other log verbosity levels (such as `debug`, `trace`, or `full`) are only for use in debugging, testing, or development scenarios.

When starting up the SurrealDB binary ensure that the `--log` argument is omitted, or specifically set to the correct log verbosity level.

```sh
surreal start --log info rocksdb:/data
```

When starting up the SurrealDB Docker container ensure that the `--log` argument is omitted, or specifically set to the correct log verbosity level.

```sh
docker run --rm --pull always -p 8000:8000 surrealdb/surrealdb:latest start --log info rocksdb:/data
```

### Running SurrealDB embedded in Rust

When running SurrealDB as an embedded database within Rust, using the correct release profile and memory allocator can greatly improve the performance of the database core engine. In addition using an optimised asynchronous runtime configuration can help speed up concurrent queries and increase database throughput.

In your project's `Cargo.toml` file, ensure that the release profile uses the following configuration:

```toml
[profile.release]
lto = true
strip = true
opt-level = 3
panic = 'abort'
codegen-units = 1
```

In your project's `Cargo.toml` file, ensure that the `allocator` feature is enabled on the `surrealdb` dependency:

```toml
surrealdb = { version = "2", features = ["allocator", "storage-mem", "storage-surrealkv", "storage-rocksdb", "protocol-http", "protocol-ws", "rustls"] }
```

When running SurrealDB within your Rust code, ensure that the asynchronous runtime is configured correctly, making use of multiple threads, an increased stack size, and an optimised number of threads:

```toml
tokio = { version = "1.41.1", features = ["sync", "rt-multi-thread"] }
```

```rs
fn main() {
  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .thread_stack_size(10 * 1024 * 1024) // 10MiB
    .build()
    .unwrap()
    .block_on(async {
      // Your application code
    })
}
```

### Running SurrealDB embedded in Tauri

When running SurrealDB as an embedded database within Rust, default options of Tauri can make SurrealDB run slower, as it processes and outputs the database information logs. Configuring Tauri correctly, can result in much improved performance with the core database engine and any queries which are run on SurrealDB.

When building a desktop application with Tauri, ensure that the Tauri plugin log is disabled by:

```toml
[profile.release]
lto = true
strip = true
opt-level = 3
panic = 'abort'
codegen-units = 1
```

### Running SurrealDB in the browser using WebAssembly

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce eget lorem mauris. Curabitur auctor tincidunt ex et lacinia. Sed et turpis viverra, porttitor dui a, varius dui. Morbi vel ex sed libero aliquam bibendum sed non magna. Nullam et sem et felis ornare accumsan vel a ipsum. Aenean hendrerit id elit congue consequat. Proin dignissim magna in sem cursus, eget varius erat suscipit. Phasellus quis ultricies lorem. Pellentesque et semper augue, eu gravida risus. Aenean hendrerit mauris vitae lectus efficitur dictum. Nullam vitae eros sed nisi euismod tempor eu nec nibh.

<br>

## Performing queries

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce eget lorem mauris. Curabitur auctor tincidunt ex et lacinia. Sed et turpis viverra, porttitor dui a, varius dui. Morbi vel ex sed libero aliquam bibendum sed non magna. Nullam et sem et felis ornare accumsan vel a ipsum. Aenean hendrerit id elit congue consequat. Proin dignissim magna in sem cursus, eget varius erat suscipit. Phasellus quis ultricies lorem. Pellentesque et semper augue, eu gravida risus. Aenean hendrerit mauris vitae lectus efficitur dictum. Nullam vitae eros sed nisi euismod tempor eu nec nibh.

### Selecting single records

Certain queries in SurrealDB can be more efficiently written in certain ways which ensure that full table scans or indexes are not necessary when executing the query.

In traditional SQL, the following query can be used to query for a particular row from a table:

```sql
SELECT * FROM user WHERE id = 19374837491;
```

However, currently in SurrealDB this query will perform a table or index scan to find the record, although this is not necessary when using SurrealDB. Instead the following query can be used to select the specific record without needing to define any indexes or perform any table scan iterations:

```sql
SELECT * FROM user:19374837491;
```

### Selecting multiple records

```sql
SELECT * FROM user WHERE id = 19374837491 OR id = 12647931632;
```

However, currently in SurrealDB this query will perform a table or index scan to find the record, although this is not necessary when using SurrealDB. Instead the following query can be used to select the specific record without needing to define any indexes or perform any table scan iterations:

```sql
SELECT * FROM user:19374837491, user:12647931632;
```

### Iterating over tables

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce eget lorem mauris. Curabitur auctor tincidunt ex et lacinia. Sed et turpis viverra, porttitor dui a, varius dui. Morbi vel ex sed libero aliquam bibendum sed non magna. Nullam et sem et felis ornare accumsan vel a ipsum. Aenean hendrerit id elit congue consequat. Proin dignissim magna in sem cursus, eget varius erat suscipit. Phasellus quis ultricies lorem. Pellentesque et semper augue, eu gravida risus. Aenean hendrerit mauris vitae lectus efficitur dictum. Nullam vitae eros sed nisi euismod tempor eu nec nibh.

### Query complex graph relationships

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce eget lorem mauris. Curabitur auctor tincidunt ex et lacinia. Sed et turpis viverra, porttitor dui a, varius dui. Morbi vel ex sed libero aliquam bibendum sed non magna. Nullam et sem et felis ornare accumsan vel a ipsum. Aenean hendrerit id elit congue consequat. Proin dignissim magna in sem cursus, eget varius erat suscipit. Phasellus quis ultricies lorem. Pellentesque et semper augue, eu gravida risus. Aenean hendrerit mauris vitae lectus efficitur dictum. Nullam vitae eros sed nisi euismod tempor eu nec nibh.

### Using indexes

SurrealDB has native built-in support for a number of different index types, without leveraging external libraries or implementations. With native support for indexes in the core database engine, SurrealDB leverages indexes where possible within the SurrealQL query language, without pushing queries down to a separate indexing engine. In addition, data is indexed in the same way for embedded systems, single-node database servers, and multi-node highly-available clusters, ensuring that the same indexing functionality is available regardless of the SurrealDB deployment model. Indexing support in SurrealDB is in active development, with work focusing on increased support for additional operators, compound indexes, additional index types, and overall improved index performance.

Currently no indexes are used when performing `UPDATE` or `DELETE` queries on large table, even where indexes are defined. This functionality will be available in a future release. In the meantime, you can improve the performance of `UPDATE` and `DELETE` statements by combining these with a `SELECT` statement:

To improve the performance of an `UPDATE` statement, use a `SELECT` statement within a subquery, selecting only the `id` field. This will use any defined indexes:

```sql
UPDATE (SELECT id FROM user WHERE age < 18) SET adult = false;
```

To improve the performance of an `DELETE` statement, use a `SELECT` statement within a subquery, selecting only the `id` field. This will use any defined indexes:

```sql
DELETE (SELECT id FROM user WHERE age < 18);
```

Supported index types:
- [x] Traditional indexes
- [x] Unique indexes
- [x] Compound indexes
- [x] Multikey array indexes
- [x] Compound, multikey array indexes
- [x] Compound, multikey, flattened array indexes
- [x] Full-text search indexes
- [x] M-tree exact vector search indexes
- [x] HNSW approximate vector search indexes
- [ ] Geospatial R-tree indexes
- [ ] Geospatial Quad-tree indexes

Index support in statements:
- [x] Indexes used when running `SELECT` statements
- [ ] Indexes used when running `UPSERT` statements
- [ ] Indexes used when running `UPDATE` statements
- [ ] Indexes used when running `DELETE` statements

Operator support in queries:
- [x] Support for using traditional indexes
- [x] Support for using unique indexes
- [x] Support for using compound indexes
- [x] Support for using multiple indexes when using `AND` in queries
- [ ] Support for using multiple indexes when using `OR` in queries

<br>

## Performance roadmap

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce eget lorem mauris. Curabitur auctor tincidunt ex et lacinia. Sed et turpis viverra, porttitor dui a, varius dui. Morbi vel ex sed libero aliquam bibendum sed non magna. Nullam et sem et felis ornare accumsan vel a ipsum. Aenean hendrerit id elit congue consequat. Proin dignissim magna in sem cursus, eget varius erat suscipit. Phasellus quis ultricies lorem. Pellentesque et semper augue, eu gravida risus. Aenean hendrerit mauris vitae lectus efficitur dictum. Nullam vitae eros sed nisi euismod tempor eu nec nibh.
