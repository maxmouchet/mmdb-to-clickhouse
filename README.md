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
- The `network` and `partition` names are reserved and must not be present in the MMDB file.
- The username and password will be embedded in the dictionary source definition (see https://github.com/ClickHouse/ClickHouse/issues/38991).
  - To avoid storing critical credentials in the dictionary definition, you can create a dedicated user that has only access to the MMDB tables.

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

(See [clickhouse-go](https://github.com/ClickHouse/clickhouse-go?tab=readme-ov-file#dsn) documentation for the DSN format and allowed values)

The output should look like the following:

```
2024/07/04 13:51:07 Net schema: `network` String, `pointer` UInt64, `partition` Date
2024/07/04 13:51:07 Val schema: `pointer` UInt32, `country` String, `partition` Date
2024/07/04 13:51:07 Creating table example_mmdb_net_history
2024/07/04 13:51:07 Creating table example_mmdb_val_history
2024/07/04 13:51:07 Creating dictionary example_mmdb_net
2024/07/04 13:51:07 Creating dictionary example_mmdb_val
2024/07/04 13:51:07 Dropping partition 2024-07-04
2024/07/04 13:51:07 Dropping partition 2024-07-04
2024/07/04 13:51:07 Inserting data
2024/07/04 13:51:07 Inserted 1 networks and 1 values
2024/07/04 13:51:07 Creating function example_mmdb
2024/07/04 13:51:07 Running test query: SELECT example_mmdb('1.1.1.1', 'country')
2024/07/04 13:51:07 This may take some time as the dictionnary gets loaded in memory
2024/07/04 13:51:07 Test query result: WW
```

This will create:
- Two [partitioned](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key) tables (30 days of history by default, see the `-ttl` option):
  - `example_mmdb_net_history`: IP networks and pointers to distinct values
  - `example_mmdb_val_history`: pointers and associated values
- Two [dictionaries](https://clickhouse.com/docs/en/sql-reference/dictionaries):
  - `example_mmdb_net`: an in-memory IP trie which always uses the latest partition from `example_mmdb_net_history`. This dictionary enables very fast IP lookups.
  - `example_mmdb_val`: an in-memory KV mapping which always uses the latest partition from `example_mmdb_val_history`.
- One [function](https://clickhouse.com/docs/en/sql-reference/statements/create/function):
  - `example_mmdb(ip, attrs)`: this function first looks up the pointer in `example_mmdb_net` and then retrieves the value in `example_mmdb_val`.

Open a REPL and inspect the tables:
```bash
docker exec -it clickhouse clickhouse client
```

```sql
SHOW TABLES
-- ┌─name─────────────────────┐
-- │ example_mmdb_net         │
-- │ example_mmdb_net_history │
-- │ example_mmdb_val         │
-- │ example_mmdb_val_history │
-- └──────────────────────────┘

SELECT * FROM example_mmdb_net
-- ┌─network───┬─pointer─┬──partition─┐
-- │ 0.0.0.0/0 │       0 │ 2024-07-04 │
-- └───────────┴─────────┴────────────┘

SELECT * FROM example_mmdb_val
-- ┌─pointer─┬─country─┬──partition─┐
-- │       0 │ WW      │ 2024-07-04 │
-- └─────────┴─────────┴────────────┘

SELECT example_mmdb('1.1.1.1', 'country') AS country
-- ┌─country─┐
-- │ WW      │
-- └─────────┘
```

To clean up just remove the ClickHouse instance:
```bash
docker rm -f clickhouse
```

Or, to cleanup tables manually:
```sql
DROP FUNCTION example_mmdb;
DROP DICTIONARY example_mmdb_net;
DROP DICTIONARY example_mmdb_val;
DROP TABLE example_mmdb_net_history;
DROP TABLE example_mmdb_val_history;
```

## Performance

Tests performed with ClickHouse 24.6.1 and mmdb-to-clickhouse 1.2.1 on a VM with 4 vCPUs (i5-12600H) and 32GB of memory.

Database | MMDB size | Networks | Values | Insertion time | Dict cold load time | Dict mem usage | Lookup
---------|-----------|----------|--------|----------------|---------------------|----------------|-------
[IPinfo Privacy](https://ipinfo.io/products/anonymous-ip-database)     | 293MB | 12M  | 387  | 30 seconds | 30 seconds | 558MB | 40M rows/s
[IPinfo Location](https://ipinfo.io/products/ip-geolocation-database)  | 1.6GB | 205M | 358k | 9 minutes  | 8 minutes  | 9GB   | 40M rows/s
