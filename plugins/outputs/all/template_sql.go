//go:build !custom || outputs || outputs.template_sql

package all

import _ "github.com/influxdata/telegraf/plugins/outputs/template_sql" // register plugin
