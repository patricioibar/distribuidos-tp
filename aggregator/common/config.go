package common

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "./config.json"

// AggConfig represents a single aggregation configuration.
type AggConfig struct {
	Col  string `json:"col"`
	Func string `json:"func"`
}

// Config represents the application's configuration structure.
type Config struct {
	AggId             string      `json:"agg-id"`
	MiddlewareAddress string      `json:"middleware-address"`
	GroupBy           []string    `json:"group-by"`
	Aggregations      []AggConfig `json:"aggregations"`
	InputName         string      `json:"input-name"`
	OutputName        string      `json:"output-name"`
	LogLevel          string      `json:"log-level"`
}

// InitConfig reads configuration from a JSON file and environment variables.
// Environment variables take precedence over the config file.
func InitConfig() (*Config, error) {
	v := viper.New()

	requiredFields := []string{
		"agg-id",
		"middleware-address",
		"group-by",
		"aggregations",
		"input-name",
		"output-name",
		"log-level",
	}

	// Set config file type and name
	v.SetConfigFile(configFilePath)
	v.SetConfigType("json")

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	for _, field := range requiredFields {
		v.BindEnv(field)
	}

	v.ReadInConfig() // ignore error, we are not using config file strictly

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
