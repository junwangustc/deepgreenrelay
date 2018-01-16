package proxy

import (
	"os"

	"github.com/naoina/toml"
)

type Config struct {
	HTTPProxys []HTTPConfig `toml:"http"`
}

type HTTPConfig struct {
	// Name identifies the HTTP relay
	Name string `toml:"name"`

	// Addr should be set to the desired listening host:port
	Addr string `toml:"bind-addr"`

	// Default retention policy to set for forwarded requests
	DefaultRetentionPolicy string `toml:"default-retention-policy"`

	// Outputs is a list of backed servers where writes will be forwarded

	Time int `toml:"time"`

	GoNum int `toml:"gonum"`

	FilterMetric []string `toml:"filter-metric"`

	BufferTimes int `toml:"buffer-times"` //次数

	BufferLen int `toml:"buffer-len"` //字节数

	Times int `toml:"times"`

	DispelMetric []Dispel `toml:"dispel-metric"`
}

type Dispel struct {
	MetricName string `toml:"metric-name"`
	TagKey     string `toml:"tag-key"`
	TagValue   string `toml:"tag-value"`
}

// LoadConfigFile parses the specified file into a Config object
func LoadConfigFile(filename string) (cfg Config, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	return cfg, toml.NewDecoder(f).Decode(&cfg)
}
