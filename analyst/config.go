package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "config.json"

type TableConfig struct {
	Name    string   `json:"name" mapstructure:"name"`
	Columns []string `json:"columns" mapstructure:"columns"`
}

// Config represents the application's configuration structure.
type Config struct {
	DataDir               string        `json:"data-dir" mapstructure:"data-dir"`
	BatchSize             int           `json:"batch-size" mapstructure:"batch-size"`
	CoffeeAnalyzerAddress string        `json:"coffee-analyzer-address" mapstructure:"coffee-analyzer-address"`
	LogLevel              string        `json:"log-level" mapstructure:"log-level"`
	Tables                []TableConfig `json:"tables" mapstructure:"tables"`
}

var requiredFields = []string{
	"data-dir",
	"batch-size",
	"coffee-analyzer-address",
	"tables",
}

// field: default value
var optionalFields = map[string]interface{}{
	"log-level": "INFO",
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

	if s := v.GetString("tables"); s != "" {
		var tables []TableConfig
		if err := json.Unmarshal([]byte(s), &tables); err != nil {
			return nil, fmt.Errorf("could not parse tables JSON: %w", err)
		}
		v.Set("tables", tables)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %w", err)
	}

	// Set defaults for optional fields if not set
	for optField, defaultValue := range optionalFields {
		if !v.IsSet(optField) {
			v.Set(optField, defaultValue)
		}
	}

	return &config, nil
}
