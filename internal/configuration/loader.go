package configuration

import (
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/configuration/util"

	"gopkg.in/yaml.v3"
)

func LoadConfig() (*properties.Config, *properties.CommandConfigProperties, *properties.RaftConfigProperties, *properties.TransportConfigProperties, error) {
	config, err := load()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load config with error: %v", err)
	}

	cfgProvider := properties.NewProvider(config)
	commandConfig := cfgProvider.GetCommand()
	raftConfig := cfgProvider.GetRaft()
	transportConfig := cfgProvider.GetTransport()
	return config, commandConfig, raftConfig, transportConfig, nil
}

func load() (*properties.Config, error) {

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
