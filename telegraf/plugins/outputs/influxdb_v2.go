package outputs

import (
	"fmt"
	"strconv"
	"strings"
)

// InfluxDBV2 is based on telegraf influxdb_v2 output plugin.
type InfluxDBV2 struct {
	baseOutput
	URLs         []string `toml:"urls"`
	Token        string   `toml:"token"`
	Organization string   `toml:"organization"`
	Bucket       string   `toml:"bucket"`
}

// PluginName is based on telegraf plugin name.
func (i *InfluxDBV2) PluginName() string {
	return "influxdb_v2"
}

// TOML encodes to toml string.
func (i *InfluxDBV2) TOML() string {
	s := make([]string, len(i.URLs))
	for k, v := range i.URLs {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[outputs.%s]]	
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:9999
  urls = [%s]

  ## Token for authentication.
  token = "%s"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "%s"

  ## Destination bucket to write into.
  bucket = "%s"
`, i.PluginName(), strings.Join(s, ", "), i.Token, i.Organization, i.Bucket)
}
