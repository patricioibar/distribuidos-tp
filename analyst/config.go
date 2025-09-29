package analyst

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "config.json"

// Config represents the application's configuration structure.
type Config struct {
	BatchSize             int    `json:"batch-size" mapstructure:"chunk-size"`
	CoffeeAnalyzerAddress string `json:"coffee-analyzer-address" mapstructure:"coffee-analyzer-address"`
	DatasetPath           string `json:"dataset-path" mapstructure:"dataset-path"`
	LogLevel              string `json:"log-level" mapstructure:"log-level"`
}

var requiredFields = []string{
	"chunk-size",
	"coffee-analyzer-address",
	"dataset-path",
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
