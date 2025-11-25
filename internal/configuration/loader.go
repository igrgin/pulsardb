package configuration

import (
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/configuration/util"

	"gopkg.in/yaml.v3"
)

func Load() (*properties.Config, error) {

	cfg, err := loadBaseConfig()
	if err != nil {
		return nil, err
	}

	err = loadProfileConfig(cfg)
	if err != nil {
		return nil, err
	}

	return cfg, err
}

func loadBaseConfig() (*properties.Config, error) {
	baseConfig, err := util.LoadAndExpandYaml("internal/static", "application")
	if err != nil {
		slog.Error("Error parsing base config", "Error", err.Error())
		return nil, err
	}

	cfg := properties.Config{}
	if err := yaml.Unmarshal([]byte(baseConfig), &cfg); err != nil {
		slog.Error("Error parsing base config", "Error", err.Error())
		return nil, err
	}

	return &cfg, nil
}

func loadProfileConfig(cfg *properties.Config) error {
	profileConfig, err := util.LoadAndExpandYaml("internal/static", fmt.Sprintf("application-%s", cfg.Application.Profile))
	if err != nil {
		slog.Error("Error loading profile config", "Error", err.Error())
		return err
	}

	if err := yaml.Unmarshal([]byte(profileConfig), &cfg); err != nil {
		slog.Error("Error parsing profile config", "Error", err.Error())
		return err
	}

	return nil
}
