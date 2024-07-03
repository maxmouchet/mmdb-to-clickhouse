package main

import (
	"fmt"
	"log"
	"math/big"
	"slices"
	"strings"

	"golang.org/x/exp/maps"
)

type column struct {
	Name string
	Kind string
}

type pair struct {
	Key   string
	Value interface{}
}

func flattenRecord(m map[string]interface{}) map[string]interface{} {
	pairs := flattenRecordRecursive(m, "")
	flattened := make(map[string]interface{})
	for _, pair := range pairs {
		flattened[pair.Key] = pair.Value
	}
	return flattened
}

func flattenRecordRecursive(m map[string]interface{}, parent string) []pair {
	pairs := make([]pair, 0)

	// Map iteration order is not guaranteed in Go, so we sort the key to ensure we always get the same order.
	keys := maps.Keys(m)
	slices.Sort(keys)

	for _, key := range keys {
		value := m[key]
		name := parent + key
		nested, ok := value.(map[string]interface{})
		if ok {
			pairs = append(pairs, flattenRecordRecursive(nested, name+"_")...)
		} else {
			pairs = append(pairs, pair{name, value})
		}
	}

	return pairs
}

func inferSchema(m map[string]interface{}, allowedColumns []string) []column {
	schema := []column{{"pointer", "UInt32"}}
	records := flattenRecordRecursive(m, "")

	for _, record := range records {
		if allowedColumns[0] != "" && !slices.Contains(allowedColumns, record.Key) {
			continue
		}
		switch record.Value.(type) {
		case bool:
			schema = append(schema, column{record.Key, "Boolean"})
		case []byte:
			// TODO: Is this the right type for ClickHouse?
			schema = append(schema, column{record.Key, "String"})
		case string:
			schema = append(schema, column{record.Key, "String"})
		case uint16:
			schema = append(schema, column{record.Key, "UInt16"})
		case uint32:
			schema = append(schema, column{record.Key, "UInt32"})
		case int32:
			schema = append(schema, column{record.Key, "Int32"})
		case uint64:
			schema = append(schema, column{record.Key, "UInt64"})
		case *big.Int:
			schema = append(schema, column{record.Key, "UInt128"})
		case float32:
			schema = append(schema, column{record.Key, "Float32"})
		case float64:
			schema = append(schema, column{record.Key, "Float64"})
		default:
			log.Fatalf("unsupported type for key %s", record.Key)
		}
	}

	schema = append(schema, column{"partition", "Date"})
	return schema
}

func schemaToString(columns []column) string {
	elems := make([]string, 0)
	for _, column := range columns {
		elems = append(elems, fmt.Sprintf("`%s` %s", column.Name, column.Kind))
	}
	return strings.Join(elems, ", ")
}
