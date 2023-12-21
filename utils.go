package main

import (
	"fmt"
	"golang.org/x/exp/maps"
	"log"
	"math/big"
	"slices"
	"strings"
)

type Column struct {
	Name string
	Kind string
}

type Pair struct {
	Key   string
	Value interface{}
}

func FlattenRecord(m map[string]interface{}) map[string]interface{} {
	pairs := FlattenRecordRecursive(m, "")
	flattened := make(map[string]interface{})
	for _, pair := range pairs {
		flattened[pair.Key] = pair.Value
	}
	return flattened
}

func FlattenRecordRecursive(m map[string]interface{}, parent string) []Pair {
	pairs := make([]Pair, 0)

	// Map iteration order is not guaranteed in Go, so we sort the key to ensure we always get the same order.
	keys := maps.Keys(m)
	slices.Sort(keys)

	for _, key := range keys {
		value := m[key]
		name := parent + key
		nested, ok := value.(map[string]interface{})
		if ok {
			pairs = append(pairs, FlattenRecordRecursive(nested, name+"_")...)
		} else {
			pairs = append(pairs, Pair{name, value})
		}
	}

	return pairs
}

func InferSchema(m map[string]interface{}) []Column {
	schema := []Column{{"network", "String"}}
	records := FlattenRecordRecursive(m, "")

	for _, record := range records {
		switch record.Value.(type) {
		case bool:
			schema = append(schema, Column{record.Key, "Boolean"})
		case []byte:
			// TODO: Is this the right type for ClickHouse?
			schema = append(schema, Column{record.Key, "String"})
		case string:
			schema = append(schema, Column{record.Key, "String"})
		case uint16:
			schema = append(schema, Column{record.Key, "UInt16"})
		case uint32:
			schema = append(schema, Column{record.Key, "UInt32"})
		case int32:
			schema = append(schema, Column{record.Key, "Int32"})
		case uint64:
			schema = append(schema, Column{record.Key, "UInt64"})
		case *big.Int:
			schema = append(schema, Column{record.Key, "UInt128"})
		case float32:
			schema = append(schema, Column{record.Key, "Float32"})
		case float64:
			schema = append(schema, Column{record.Key, "Float64"})
		default:
			log.Fatalf("unsupported type for key %s", record.Key)
		}
	}

	schema = append(schema, Column{"partition", "Date"})
	return schema
}

func SchemaToString(columns []Column) string {
	elems := make([]string, 0)
	for _, column := range columns {
		elems = append(elems, fmt.Sprintf("`%s` %s", column.Name, column.Kind))
	}
	return strings.Join(elems, ", ")
}
