# Benchmarks

A place for all kinds of benchmarking projects

## go-ycsb
Cloned from https://github.com/pingcap/go-ycsb and added `db/surrealdb`

To run it against a local surrealdb instance (`ws://localhost:8000/rpc`):
```
$ cd go-ycsb
$ make build
$ ./bin/go-ycsb run surrealdb -P workloads/basic
```

Available config properties (see go-ycsb/db/surrealdb/db.go:29):
```
$ ./bin/go-ycsb run surrealdb -P workloads/basic -p surrealdb.uri='ws://localhost:8000/rpc' -p surrealdb.user=root -p surrealdb.pass=root -p surrealdb.ns=ycsb -p surrealdb.db=ycsb
```

### FoundationDB
On a compatible system, follow these steps:
* Install foundationdb-clients=7.1.37 (https://github.com/apple/foundationdb/releases/download/7.1.37/foundationdb-clients_7.1.37-1_amd64.deb)
* `make build`
* `./bin/go-ycsb run foundationdb -P workloads/basic`
