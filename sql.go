package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func createDict(
	ctx context.Context,
	conn driver.Conn,
	name string,
	schema string,
	source string,
	primaryKey string,
	layout string,
	user string,
	password string,
) error {
	log.Printf("Creating dictionary %s", name)
	if user == "" {
		user = "default"
	}
	query := fmt.Sprintf(`
		CREATE DICTIONARY IF NOT EXISTS %s (%s)
		PRIMARY KEY %s
		SOURCE(CLICKHOUSE(
			QUERY 'SELECT * FROM %s WHERE partition = (SELECT MAX(partition) FROM %s)'
			USER '%s'
			PASSWORD '%s'
		))
		LIFETIME(MIN 0 MAX 3600)
		LAYOUT(%s)
	`, name, schema, primaryKey, source, source, user, password, layout)
	// TODO: Use named parameters wherever possible
	return conn.Exec(ctx, query)
}

func createFunction(
	ctx context.Context,
	conn driver.Conn,
	name string,
	args string,
	expr string,
) error {
	log.Printf("Creating function %s", name)
	query := fmt.Sprintf(`
	CREATE FUNCTION IF NOT EXISTS %s AS (%s) -> %s
`, name, args, expr)
	// TODO: Use named parameters wherever possible
	return conn.Exec(ctx, query)
}

func createPartitionedTable(
	ctx context.Context,
	conn driver.Conn,
	name string,
	schema string,
	orderBy string,
	ttl int,
) error {
	log.Printf("Creating table %s", name)
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS {name:Identifier} (%s)
		ENGINE MergeTree
		ORDER BY {orderBy:Identifier}
		PARTITION BY partition
		TTL partition + INTERVAL {ttl:Int64} DAY
	`, schema)
	return conn.Exec(
		ctx,
		query,
		clickhouse.Named("name", name),
		clickhouse.Named("orderBy", orderBy),
		clickhouse.Named("ttl", strconv.Itoa(ttl)),
	)
}

func dropDict(ctx context.Context, conn driver.Conn, name string) error {
	log.Printf("Dropping dictionary %s", name)
	query := "DROP DICTIONARY IF EXISTS {name:Identifier}"
	return conn.Exec(ctx, query, clickhouse.Named("name", name))
}

func dropFunction(ctx context.Context, conn driver.Conn, name string) error {
	log.Printf("Dropping function %s", name)
	// CH doesn't support named parameters for DROP FUNCTION.
	query := fmt.Sprintf("DROP FUNCTION IF EXISTS %s", name)
	return conn.Exec(ctx, query)
}

func dropPartition(ctx context.Context, conn driver.Conn, name string, partition string) error {
	log.Printf("Dropping partition %s", partition)
	query := "ALTER TABLE {name:Identifier} DROP PARTITION {partition:String}"
	return conn.Exec(ctx, query, clickhouse.Named("name", name), clickhouse.Named("partition", partition))
}

func dropTable(ctx context.Context, conn driver.Conn, name string) error {
	log.Printf("Dropping table %s", name)
	query := "DROP TABLE IF EXISTS {name:Identifier}"
	return conn.Exec(ctx, query, clickhouse.Named("name", name))
}

func reloadDict(ctx context.Context, conn driver.Conn, name string) error {
	log.Printf("Reloading dictionary %s", name)
	query := "SYSTEM RELOAD DICTIONARY {name:Identifier}"
	return conn.Exec(ctx, query, clickhouse.Named("name", name))
}

func prepareBatch(ctx context.Context, conn driver.Conn, name string) (driver.Batch, error) {
	return conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", name))
}
