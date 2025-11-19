package initializer

import (
	applicationConfig "pulsardb/config/application"
	"pulsardb/config/loader"
)

func Initialize(baseDir string, profileDir string, loglevel string) (*applicationConfig.Config, error) {
	configLoader := loader.NewConfigLoader(baseDir, profileDir, loglevel)
	return configLoader.Load()
}
