package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/oschwald/maxminddb-golang"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	dsn := flag.String("dsn", "clickhouse://localhost:9000", "ClickHouse URL")
	mmdb := flag.String("mmdb", "example.mmdb", "MMDB file path")
	name := flag.String("name", "example_mmdb", "UDF name")
	partition := flag.String("partition", time.Now().Format("2006-01-02"), "Partition date")
	batchSize := flag.Int("batch", 1_000_000, "Number of rows to insert at once")
	allowedColumns := flag.String("columns", "", "Comma-separated list of columns to import (all by default)")
	drop := flag.Bool("drop", false, "Drop previous tables if they exist. Regardless of this flag, the current partition is always dropped to ensure idempotence.")
	reload := flag.Bool("reload", false, "Reload the dictionary to ensure the new data is used right after insertion")
	test := flag.Bool("test", false, "Run a test query after insertion to ensure the dict is working as expected")
	ttl := flag.Int("ttl", 30, "Number of partitions (days) to keep. This is not updated if the tables already exist.")

	flag.Parse()
	ctx := context.Background()

	netDict := fmt.Sprintf("%s_net", *name)
	valDict := fmt.Sprintf("%s_val", *name)
	netTable := fmt.Sprintf("%s_net_history", *name)
	valTable := fmt.Sprintf("%s_val_history", *name)

	options, err := clickhouse.ParseDSN(*dsn)
	check(err)

	qualifiedNetDict := fmt.Sprintf("%s", netDict)
	qualifiedValDict := fmt.Sprintf("%s", valDict)
	qualifiedNetTable := fmt.Sprintf("`%s`", netTable)
	qualifiedValTable := fmt.Sprintf("`%s`", valTable)
	if options.Auth.Database != "" {
		qualifiedNetDict = fmt.Sprintf("%s.%s", options.Auth.Database, netDict)
		qualifiedValDict = fmt.Sprintf("%s.%s", options.Auth.Database, valDict)
		qualifiedNetTable = fmt.Sprintf("`%s`.`%s`", options.Auth.Database, netTable)
		qualifiedValTable = fmt.Sprintf("`%s`.`%s`", options.Auth.Database, valTable)
	}

	conn, err := clickhouse.Open(options)
	check(err)
	defer conn.Close()

	db, err := maxminddb.Open(*mmdb)
	check(err)
	defer db.Close()

	record := make(map[string]interface{})
	err = db.Decode(0, &record)
	check(err)

	netSchemaStr := "`network` String, `pointer` UInt64, `partition` Date"
	log.Printf("Net schema: %s", netSchemaStr)

	valSchema := inferSchema(record, strings.Split(*allowedColumns, ","))
	valSchemaStr := schemaToString(valSchema)
	log.Printf("Val schema: %s", valSchemaStr)

	if *drop {
		check(dropFunction(ctx, conn, *name))
		check(dropDict(ctx, conn, netDict))
		check(dropDict(ctx, conn, valDict))
		check(dropTable(ctx, conn, netTable))
		check(dropTable(ctx, conn, valTable))
	}

	check(createPartitionedTable(ctx, conn, netTable, netSchemaStr, "network", *ttl))
	check(createPartitionedTable(ctx, conn, valTable, valSchemaStr, "pointer", *ttl))

	check(createDict(ctx, conn, netDict, netSchemaStr, qualifiedNetTable, "network", "IP_TRIE", options.Auth.Username, options.Auth.Password))
	check(createDict(ctx, conn, valDict, valSchemaStr, qualifiedValTable, "pointer", "FLAT", options.Auth.Username, options.Auth.Password))

	check(dropPartition(ctx, conn, netTable, *partition))
	check(dropPartition(ctx, conn, valTable, *partition))

	log.Printf("Inserting data")
	networks := db.Networks(maxminddb.SkipAliasedNetworks)

	netBatch, err := prepareBatch(ctx, conn, netTable)
	check(err)
	valBatch, err := prepareBatch(ctx, conn, valTable)
	check(err)

	netCount := 0
	valCount := 0
	offsetToPointer := make(map[uintptr]int)

	for networks.Next() {
		network, err := networks.Network(&record)
		check(err)

		offset, err := db.LookupOffset(network.IP)
		check(err)

		if _, ok := offsetToPointer[offset]; !ok {
			pointer := valCount + 1 // Reserve index 0 for unknown values
			offsetToPointer[offset] = pointer
			flattened := flattenRecord(record)
			val := []interface{}{pointer}
			for _, column := range valSchema {
				if column.Name != "pointer" && column.Name != "partition" {
					val = append(val, flattened[column.Name])
				}
			}
			val = append(val, partition)
			check(valBatch.Append(val...))
			valCount += 1
		}

		err = netBatch.Append(network, offsetToPointer[offset], partition)
		check(err)

		netCount += 1
		if netCount%*batchSize == 0 {
			check(netBatch.Send())
			check(valBatch.Send())
			netBatch, err = prepareBatch(ctx, conn, netTable)
			check(err)
			valBatch, err = prepareBatch(ctx, conn, valTable)
			check(err)
			log.Printf("Inserted %d networks and %d values", netCount, valCount)
		}
	}

	check(netBatch.Send())
	check(valBatch.Send())
	log.Printf("Inserted %d networks and %d values", netCount, valCount)

	createFunction(ctx, conn, *name, "ip, attrs", fmt.Sprintf(
		"dictGet('%s', attrs, dictGet('%s', 'pointer', toIPv6(ip)))",
		qualifiedValDict,
		qualifiedNetDict,
	))

	if *reload {
		check(reloadDict(ctx, conn, netDict))
		check(reloadDict(ctx, conn, valDict))
	}

	if *test {
		query := fmt.Sprintf("SELECT %s('1.1.1.1', '%s')", *name, valSchema[1].Name)
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
