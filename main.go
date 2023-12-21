package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/oschwald/maxminddb-golang"
	"log"
	"strconv"
	"time"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	dsn := flag.String("dsn", "clickhouse://localhost:9000", "ClickHouse URL")
	mmdb := flag.String("mmdb", "example.mmdb", "MMDB file path")
	name := flag.String("name", "example_mmdb", "Table name")
	partition := flag.String("partition", time.Now().Format("2006-01-02"), "Partition date")
	batchSize := flag.Int("batch", 1_000_000, "Number of rows to insert at once")
	drop := flag.Bool("drop", false, "Drop previous tables if they exist. Regardless of this flag, the current partition is always dropped to ensure idempotence.")
	reload := flag.Bool("reload", false, "Reload the dictionary to ensure the new data is used right after insertion")
	test := flag.Bool("test", false, "Run a test query after insertion to ensure the dict is working as expected")
	ttl := flag.Int("ttl", 30, "Number of partitions (days) to keep. This is not updated if the tables already exist.")

	flag.Parse()
	ctx := context.Background()

	tableName := fmt.Sprintf("%s_history", *name)
	dictName := *name

	ttlValue := clickhouse.Named("ttl", strconv.Itoa(*ttl))
	dictValue := clickhouse.Named("dict", dictName)
	tableValue := clickhouse.Named("table", tableName)
	partitionValue := clickhouse.Named("partition", *partition)

	options, err := clickhouse.ParseDSN(*dsn)
	check(err)

	conn, err := clickhouse.Open(options)
	check(err)
	defer conn.Close()

	db, err := maxminddb.Open(*mmdb)
	check(err)
	defer db.Close()

	record := make(map[string]interface{})
	err = db.Decode(0, &record)
	check(err)

	schema := InferSchema(record)
	schemaStr := SchemaToString(schema)
	log.Printf("Schema: %s", schemaStr)

	if *drop {
		log.Printf("Dropping %s", tableName)
		query := "DROP TABLE IF EXISTS {table:Identifier}"
		err = conn.Exec(ctx, query, tableValue)
		check(err)

		log.Printf("Dropping %s", dictName)
		query = "DROP DICTIONARY IF EXISTS {dict:Identifier}"
		err = conn.Exec(ctx, query, dictValue)
		check(err)
	}

	log.Printf("Creating %s", tableName)
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS {table:Identifier} (%s)
		ENGINE MergeTree
		ORDER BY network
		PARTITION BY partition
		TTL partition + INTERVAL {ttl:Int64} DAY
	`, schemaStr)
	err = conn.Exec(ctx, query, tableValue, ttlValue)
	check(err)

	log.Printf("Creating %s", dictName)
	query = fmt.Sprintf(`
		CREATE DICTIONARY IF NOT EXISTS {dict:Identifier} (%s)
		PRIMARY KEY network
		SOURCE(CLICKHOUSE(QUERY '
			SELECT *
			FROM %s
			WHERE partition = (SELECT MAX(partition) FROM %s)
		'))
		LIFETIME(MIN 0 MAX 3600)
		LAYOUT(IP_TRIE)
	`, schemaStr, tableName, tableName)
	err = conn.Exec(ctx, query, dictValue)
	check(err)

	log.Printf("Dropping partition %s", *partition)
	query = "ALTER TABLE {table:Identifier} DROP PARTITION {partition:String}"
	err = conn.Exec(ctx, query, tableValue, partitionValue)
	check(err)

	networks := db.Networks(maxminddb.SkipAliasedNetworks)
	query = fmt.Sprintf("INSERT INTO %s", tableName)

	i := 0
	batch, err := conn.PrepareBatch(ctx, query)
	check(err)

	for networks.Next() {
		network, err := networks.Network(&record)
		check(err)

		flattened := FlattenRecord(record)
		vals := []interface{}{network}
		for _, column := range schema {
			if column.Name != "network" && column.Name != "partition" {
				vals = append(vals, flattened[column.Name])
			}
		}
		vals = append(vals, partition)

		err = batch.Append(vals...)
		check(err)

		i += 1
		if i%*batchSize == 0 {
			err = batch.Send()
			check(err)
			batch, err = conn.PrepareBatch(ctx, query)
			check(err)
			log.Printf("Inserted %d rows", i)
		}
	}

	err = batch.Send()
	check(err)
	log.Printf("Inserted %d rows", i)

	if *reload {
		log.Printf("Reloading %s", dictName)
		query = "SYSTEM RELOAD DICTIONARY {dict:Identifier}"
		err = conn.Exec(ctx, query, dictValue)
		check(err)
	}

	if *test {
		query = fmt.Sprintf("SELECT dictGet('%s', '%s', IPv6StringToNum('1.1.1.1'))", dictName, schema[1].Name)
		log.Printf("Running test query: %s", query)
		log.Printf("This may take some time as the dictionnary gets loaded in memory")
		var val string
		err = conn.QueryRow(ctx, query).Scan(&val)
		check(err)
		log.Printf("Test query result: %s", val)
	}

	if networks.Err() != nil {
		log.Fatal(networks.Err())
	}
}
