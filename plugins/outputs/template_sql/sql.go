//go:generate ../../../tools/readme_config_includer/generator
package template_sql

import (
	gosql "database/sql"
	_ "embed"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	//Register sql drivers
	_ "github.com/ClickHouse/clickhouse-go" // clickhouse
	_ "github.com/go-sql-driver/mysql"      // mysql
	_ "github.com/jackc/pgx/v4/stdlib"      // pgx (postgres)
	_ "github.com/microsoft/go-mssqldb"     // mssql (sql server)
	_ "github.com/snowflakedb/gosnowflake"  // snowflake

	// Register integrated auth for mssql
	_ "github.com/microsoft/go-mssqldb/integratedauth/krb5"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type ConvertStruct struct {
	Integer         string `toml:"integer"`
	Real            string `toml:"real"`
	Text            string `toml:"text"`
	Timestamp       string `toml:"timestamp"`
	Defaultvalue    string `toml:"defaultvalue"`
	Unsigned        string `toml:"unsigned"`
	Bool            string `toml:"bool"`
	ConversionStyle string `toml:"conversion_style"`
}

type SQL struct {
	Driver                string          `toml:"driver"`
	DataSourceName        string          `toml:"data_source_name"`
	InitSQL               string          `toml:"init_sql"`
	Queries               []string        `toml:"queries"`
	DefaultValues         map[string]any  `toml:"default_values"`
	Convert               ConvertStruct   `toml:"convert"`
	ConnectionMaxIdleTime config.Duration `toml:"connection_max_idle_time"`
	ConnectionMaxLifetime config.Duration `toml:"connection_max_lifetime"`
	ConnectionMaxIdle     int             `toml:"connection_max_idle"`
	ConnectionMaxOpen     int             `toml:"connection_max_open"`
	Log                   telegraf.Logger `toml:"-"`

	db *gosql.DB
}

func (*SQL) SampleConfig() string {
	return sampleConfig
}

func (p *SQL) Connect() error {
	db, err := gosql.Open(p.Driver, p.DataSourceName)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	db.SetConnMaxIdleTime(time.Duration(p.ConnectionMaxIdleTime))
	db.SetConnMaxLifetime(time.Duration(p.ConnectionMaxLifetime))
	db.SetMaxIdleConns(p.ConnectionMaxIdle)
	db.SetMaxOpenConns(p.ConnectionMaxOpen)

	if p.InitSQL != "" {
		_, err = db.Exec(p.InitSQL)
		if err != nil {
			return err
		}
	}

	p.db = db

	return nil
}

func (p *SQL) Close() error {
	return p.db.Close()
}

// Quote an identifier (table or column name)
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(sanitizeQuoted(name), `"`, `""`) + `"`
}

// Quote a string literal
func quoteStr(name string) string {
	return "'" + strings.ReplaceAll(name, "'", "''") + "'"
}

func sanitizeQuoted(in string) string {
	// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

	// Whitelist allowed characters
	return strings.Map(func(r rune) rune {
		switch {
		case r >= '\u0001' && r <= '\uFFFF':
			return r
		default:
			return '_'
		}
	}, in)
}

func (p *SQL) deriveDatatype(value interface{}) string {
	var datatype string

	switch value.(type) {
	case int64:
		datatype = p.Convert.Integer
	case uint64:
		if p.Convert.ConversionStyle == "unsigned_suffix" {
			datatype = fmt.Sprintf("%s %s", p.Convert.Integer, p.Convert.Unsigned)
		} else if p.Convert.ConversionStyle == "literal" {
			datatype = p.Convert.Unsigned
		} else {
			p.Log.Errorf("unknown conversion style: %s", p.Convert.ConversionStyle)
		}
	case float64:
		datatype = p.Convert.Real
	case string:
		datatype = p.Convert.Text
	case bool:
		datatype = p.Convert.Bool
	default:
		datatype = p.Convert.Defaultvalue
		p.Log.Errorf("Unknown datatype: '%T' %v", value, value)
	}
	return datatype
}

func (p *SQL) generateQuery(sql string, valuesMap map[string]interface{}) (string, []any, error) {

	var values []any

	r, _ := regexp.Compile(":[a-zA-Z0-9_]*")
	matchs := r.FindAllString(sql, -1)

	for _, match := range matchs {
		key := match[1:]
		value, ok := valuesMap[key]
		if !ok {
			return "", nil, fmt.Errorf("%v not found in metric", key)
		}
		values = append(values, value)
	}

	if p.Driver == "pgx" {
		// Postgres uses $1 $2 $3 as placeholders
		i := 0
		sql = r.ReplaceAllStringFunc(sql, func(s string) string {
			i++
			return "$" + strconv.Itoa(i)
		})
	} else {
		// Everything else uses ? ? ? as placeholders
		sql = r.ReplaceAllString(sql, "?")
	}

	return sql, values, nil
}

func (p *SQL) WriteMetric(metric telegraf.Metric) error {
	for _, query := range p.Queries {

		valuesMap := make(map[string]any)

		valuesMap["metric"] = metric.Name()
		valuesMap["timestamp"] = metric.Time()

		for key, value := range p.DefaultValues {
			valuesMap[key] = value
		}

		for tag, value := range metric.Tags() {
			valuesMap[tag] = value
		}

		for field, value := range metric.Fields() {
			valuesMap[field] = value
		}

		sql, values, err := p.generateQuery(query, valuesMap)
		if err != nil {
			return err
		}

		switch p.Driver {
		case "clickhouse":
			// ClickHouse needs to batch inserts with prepared statements
			tx, err := p.db.Begin()
			if err != nil {
				return fmt.Errorf("begin failed: %w", err)
			}
			stmt, err := tx.Prepare(sql)
			if err != nil {
				return fmt.Errorf("prepare failed: %w", err)
			}
			defer stmt.Close() //nolint:revive,gocritic // done on purpose, closing will be executed properly

			_, err = stmt.Exec(values...)
			if err != nil {
				return fmt.Errorf("execution failed: %w", err)
			}
			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("commit failed: %w", err)
			}
		default:
			_, err = p.db.Exec(sql, values...)
			if err != nil {
				return fmt.Errorf("execution failed: %w", err)
			}
		}
	}
	return nil
}

func (p *SQL) Write(metrics []telegraf.Metric) error {
	for _, metric := range metrics {
		err := p.WriteMetric(metric)
		if err != nil {
			return err
		}
	}
	return nil
}

func init() {
	outputs.Add("template_sql", func() telegraf.Output { return newSQL() })
}

func newSQL() *SQL {
	return &SQL{
		Convert: ConvertStruct{
			Integer:         "INT",
			Real:            "DOUBLE",
			Text:            "TEXT",
			Timestamp:       "TIMESTAMP",
			Defaultvalue:    "TEXT",
			Unsigned:        "UNSIGNED",
			Bool:            "BOOL",
			ConversionStyle: "unsigned_suffix",
		},
		// Defaults for the connection settings (ConnectionMaxIdleTime,
		// ConnectionMaxLifetime, ConnectionMaxIdle, and ConnectionMaxOpen)
		// mirror the golang defaults. As of go 1.18 all of them default to 0
		// except max idle connections which is 2. See
		// https://pkg.go.dev/database/sql#DB.SetMaxIdleConns
		ConnectionMaxIdle: 2,
	}
}
