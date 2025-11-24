package configuration

import (
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/configuration/util"
)

func Load() (*properties.Config, error) {

	cfg, err := util.LoadBaseConfig()
	if err != nil {
		return nil, err
	}

	err = util.LoadProfileConfig(cfg)
	if err != nil {
		return nil, err
	}

	return cfg, err
}
