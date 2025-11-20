package loader

import (
	"log/slog"
	applicationConfig "pulsardb/config/application"
)

type ConfigLoader struct {
	baseDir    string
	profileDir string
	loglevel   string
	logger     *slog.Logger
	baseConfig *applicationConfig.Config
	profile    string
}

func NewConfigLoader(baseDir, profileDir, loglevel string) *ConfigLoader {
	return &ConfigLoader{
		baseDir:    baseDir,
		profileDir: profileDir,
		loglevel:   loglevel,
	}
}

func (cl *ConfigLoader) Load() (*applicationConfig.Config, error) {
	if err := cl.initializeLogger(); err != nil {
		return nil, err
	}

	if err := cl.loadBaseConfig(); err != nil {
		return nil, err
	}

	if err := cl.loadProfileConfig(); err != nil {
		return nil, err
	}

	return cl.baseConfig, nil
}
