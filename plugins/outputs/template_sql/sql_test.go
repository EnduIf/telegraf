package template_sql

import (
	"testing"
)

func TestSqlTemplating(t *testing.T) {
	p := newSQL()
	p.Driver = "pgx"
	p.Query = "UPDATE :metric SET name=:name"
	result := "UPDATE $1 SET name=$2"

	valueMap := make(map[string]interface{})
	valueMap["name"] = "telegraf"
	valueMap["metric"] = "users"
	valueMap["unused"] = "unused"

	query, values, err := p.generateQuery(valueMap)
	if err != nil {
		t.Error(err)
	}

	if query != result {
		t.Error("Query does not match", query, result)
	}
	if values[0] != "users" {
		t.Error("Values does not match", values[0])
	}
	if values[1] != "telegraf" {
		t.Error("Values does not match", values[1])
	}
}

func TestSqlTemplatingFailIfNotInMetric(t *testing.T) {
	p := newSQL()
	p.Driver = "pgx"
	p.Query = "UPDATE :metric SET name=:name"

	valueMap := make(map[string]interface{})
	valueMap["metric"] = "users"
	valueMap["unused"] = "unused"

	_, _, err := p.generateQuery(valueMap)
	if err == nil {
		t.Error("Expected error when template value is not in metric")
	}
}
