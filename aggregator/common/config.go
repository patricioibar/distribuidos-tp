package common

import (
	ag "aggregator/common/aggFunctions"
	dr "aggregator/common/dataRetainer"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "config.json"

// Config represents the application's configuration structure.
type Config struct {
	WorkerId          string         `json:"worker-id" mapstructure:"worker-id"`
	WorkersCount      int            `json:"workers-count" mapstructure:"workers-count"`
	MiddlewareAddress string         `json:"middleware-address" mapstructure:"middleware-address"`
	GroupBy           []string       `json:"group-by" mapstructure:"group-by"`
	Aggregations      []ag.AggConfig `json:"aggregations" mapstructure:"aggregations"`
	QueryName         string         `json:"query-name" mapstructure:"query-name"`
	InputName         string         `json:"input-name" mapstructure:"input-name"`
	OutputName        string         `json:"output-name" mapstructure:"output-name"`
	LogLevel          string         `json:"log-level" mapstructure:"log-level"`
	BatchSize         int            `json:"output-batch-size" mapstructure:"output-batch-size"`
	IsReducer         bool           `json:"is-reducer" mapstructure:"is-reducer"`
	Retainings        []dr.Retaining `json:"retainings" mapstructure:"retainings"`
	DropNa            bool           `json:"drop-na" mapstructure:"drop-na"`
	MonitorsCount     string         `json:"monitors-count" mapstructure:"monitors-count"`
}

var requiredFields = []string{
	"worker-id",
	"workers-count",
	"middleware-address",
	"group-by",
	"aggregations",
	"query-name",
	"input-name",
	"output-name",
	"log-level",
	"output-batch-size",
	"is-reducer",
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

	// Parse complex fields from JSON env vars
	if s := v.GetString("aggregations"); s != "" {
		var aggs []ag.AggConfig
		if err := json.Unmarshal([]byte(s), &aggs); err != nil {
			return nil, fmt.Errorf("could not parse aggregations JSON: %w", err)
		}
		v.Set("aggregations", aggs)
	}

	if s := v.GetString("retainings"); s != "" {
		var topRetains []dr.Retaining
		if err := json.Unmarshal([]byte(s), &topRetains); err != nil {
			return nil, fmt.Errorf("could not parse retainings JSON: %w", err)
		}
		v.Set("retainings", topRetains)
	}

	if s := v.GetString("group-by"); s != "" {
		var groups []string
		if err := json.Unmarshal([]byte(s), &groups); err == nil {
			v.Set("group-by", groups)
		} else {
			parts := strings.Split(s, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			v.Set("group-by", parts)
		}
	}

	if !v.IsSet("drop-na") {
		v.Set("drop-na", false)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %w", err)
	}

	return &config, nil
}
