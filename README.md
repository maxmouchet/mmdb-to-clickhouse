# mmdb-to-clickhouse

[![Release](https://github.com/maxmouchet/mmdb-to-clickhouse/actions/workflows/release.yaml/badge.svg)](https://github.com/maxmouchet/mmdb-to-clickhouse/actions/workflows/release.yaml)
[![Test](https://github.com/maxmouchet/mmdb-to-clickhouse/actions/workflows/test.yaml/badge.svg)](https://github.com/maxmouchet/mmdb-to-clickhouse/actions/workflows/test.yaml)

Import MMDB files into ClickHouse.

This repository contains a dummy `example.mmdb` file for testing purpose.
To get real data, check out the free [Country & ASN database](https://ipinfo.io/products/free-ip-database) from [<img src="https://ipinfo.io/static/ipinfo-small.svg" alt="IPinfo" width="12"/> IPinfo](https://ipinfo.io) which is also supported by this tool.

## Features

- Automatically infers the table schema from the MMDB file
- Supports nested records by flattening them
- Stores data in a partitioned table to keep track of history
- Creates an IP trie dictionary for fast lookups

## Current limitations

- The schema is inferred from the first record only. If subsequent records have additional fields, those will be ignored.
- The `network` and `partition` names are reserved and must not be present in the MMDB file

## Installation

Download the latest [release](https://github.com/maxmouchet/mmdb-to-clickhouse/releases/latest) for your operating system and run:
```bash
./mmdb-to-clickhouse -h
```

You can also run it through Docker with:
```bash
docker run --rm -it ghcr.io/maxmouchet/mmdb-to-clickhouse -h
```

## Example usage


First start a ClickHouse instance:
```bash
docker run --name clickhouse --rm -d -p 9000:9000 clickhouse/clickhouse-server
```

Then download the example MMDB file:
```bash
wget https://github.com/maxmouchet/mmdb-to-clickhouse/raw/main/example.mmdb
```

And run `mmdb-to-clickhouse`:
```bash
./mmdb-to-clickhouse -dsn clickhouse://localhost:9000 -mmdb example.mmdb -name example_mmdb -test
```

The output should look like the following:

```
2023/12/21 12:18:10 Schema: network String, country String, partition Date
2023/12/21 12:18:10 Creating example_mmdb_history
2023/12/21 12:18:10 Creating example_mmdb
2023/12/21 12:18:10 Dropping partition 2023-12-21
2023/12/21 12:18:10 Inserted 1 rows
2023/12/21 12:18:10 Running test query: SELECT dictGet('example_mmdb', 'country', IPv6StringToNum('1.1.1.1'))
2023/12/21 12:18:10 Test query result: WW
```

This will create two tables:
- `example_mmdb_history`: a [partitioned](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key) table which keeps the last 30 days of history by default (see the `-ttl` option)
- `example_mmdb`: an in-memory IP trie [dictionary](https://clickhouse.com/docs/en/sql-reference/dictionaries) which always uses the latest partition from `example_mmdb_history`. This dictionary enables very fast IP lookups.

Open a REPL and inspect the tables:
```bash
docker exec -it clickhouse clickhouse client
```

```sql
SHOW TABLES
-- ┌─name─────────────────┐
-- │ example_mmdb         │
-- │ example_mmdb_history │
-- └──────────────────────┘

SELECT * FROM example_mmdb_history
-- ┌─network───┬─country─┬──partition─┐
-- │ 0.0.0.0/0 │ WW      │ 2023-12-21 │
-- └───────────┴─────────┴────────────┘

SELECT * FROM example_mmdb
-- ┌─network───┬─country─┬──partition─┐
-- │ 0.0.0.0/0 │ WW      │ 2023-12-21 │
-- └───────────┴─────────┴────────────┘

SELECT dictGet('example_mmdb', 'country', IPv6StringToNum('1.1.1.1')) AS country
-- ┌─country─┐
-- │ WW      │
-- └─────────┘
```

To clean up just remove the ClickHouse instance:
```bash
docker rm -f clickhouse
```
