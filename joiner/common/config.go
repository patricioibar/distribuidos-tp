package common

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "config.json"

// Config represents the application's configuration structure.
type Config struct {
	WorkerId          string   `json:"worker-id" mapstructure:"worker-id"`
	WorkersCount      int      `json:"workers-count" mapstructure:"workers-count"`
	MiddlewareAddress string   `json:"middleware-address" mapstructure:"middleware-address"`
	JoinKey           string   `json:"join-key" mapstructure:"join-key"`
	RightInputName    string   `json:"right-input-name" mapstructure:"right-input-name"`
	LeftInputName     string   `json:"left-input-name" mapstructure:"left-input-name"`
	OutputName        string   `json:"output-name" mapstructure:"output-name"`
	LogLevel          string   `json:"log-level" mapstructure:"log-level"`
	BatchSize         int      `json:"output-batch-size" mapstructure:"output-batch-size"`
	OutputColumns     []string `json:"output-columns" mapstructure:"output-columns"`
	QueryName         string   `json:"query-name" mapstructure:"query-name"`
	MonitorsCount     string   `json:"monitors-count" mapstructure:"monitors-count"`
}

var requiredFields = []string{
	"worker-id",
	"workers-count",
	"middleware-address",
	"join-key",
	"query-name",
	"right-input-name",
	"left-input-name",
	"output-name",
	"log-level",
	"output-batch-size",
	"output-columns",
	"monitors-count",
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
		// ignore error if config file is not found
		// as we can get all config from env vars
		if !strings.Contains(err.Error(), configFilePath) {
			return nil, fmt.Errorf("could not read config: %w", err)
		}
	}

	for _, field := range requiredFields {
		if !v.IsSet(field) {
			return nil, fmt.Errorf("missing required config field: %s", field)
		}
	}

	if s := v.GetString("output-columns"); s != "" {
		var parsed []string
		if err := json.Unmarshal([]byte(s), &parsed); err == nil {
			v.Set("output-columns", parsed)
		} else {
			parts := strings.Split(s, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			v.Set("output-columns", parts)
		}
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %w", err)
	}

	return &config, nil
}
