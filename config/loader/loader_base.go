package loader

import (
	"log/slog"
	"pulsardb/config/application"
	"pulsardb/config/util"

	"gopkg.in/yaml.v3"
)

func (cl *ConfigLoader) loadBaseConfig() error {
	baseConfig, err := util.LoadAndExpandYaml(cl.baseDir, "application")
	if err != nil {
		slog.Error("Error loading base config " + err.Error())
		return err
	}

	var cfg applicationConfig.Config
	if err := yaml.Unmarshal([]byte(baseConfig), &cfg); err != nil {
		slog.Error("Error parsing base config " + err.Error())
		return err
	}

	cl.baseConfig = &cfg
	cl.profile = cfg.Meta.Profile
	return nil
}
