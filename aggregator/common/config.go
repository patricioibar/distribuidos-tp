package common

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "config.json"

// AggConfig represents a single aggregation configuration.
type AggConfig struct {
	Col  string `json:"col" mapstructure:"col"`
	Func string `json:"func" mapstructure:"func"`
}

// Config represents the application's configuration structure.
type Config struct {
	AggId             string      `json:"agg-id" mapstructure:"agg-id"`
	MiddlewareAddress string      `json:"middleware-address" mapstructure:"middleware-address"`
	GroupBy           []string    `json:"group-by" mapstructure:"group-by"`
	Aggregations      []AggConfig `json:"aggregations" mapstructure:"aggregations"`
	QueryName         string      `json:"query-name" mapstructure:"query-name"`
	InputName         string      `json:"input-name" mapstructure:"input-name"`
	OutputName        string      `json:"output-name" mapstructure:"output-name"`
	LogLevel          string      `json:"log-level" mapstructure:"log-level"`
}

var requiredFields = []string{
	"agg-id",
	"middleware-address",
	"group-by",
	"aggregations",
	"query-name",
	"input-name",
	"output-name",
	"log-level",
}

// InitConfig reads configuration from a JSON file and environment variables.
// Environment variables take precedence over the config file.
func InitConfig() (*Config, error) {
	v := viper.New()

	// Set config file type and name
	v.SetConfigFile(configFilePath)
	v.SetConfigType("json")

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	for _, field := range requiredFields {
		v.BindEnv(field)
	}

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("could not read config: %w", err)
	}

	for _, field := range requiredFields {
		if !v.IsSet(field) {
			return nil, fmt.Errorf("missing required config field: %s", field)
		}
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %w", err)
	}

	return &config, nil
}
