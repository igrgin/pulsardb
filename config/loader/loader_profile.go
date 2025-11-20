package loader

import (
	"errors"
	"log/slog"
	"pulsardb/config/util"

	"gopkg.in/yaml.v3"
)

func validateProfile(profile, profileDir string) error {
	if profile == "" || profileDir == "" {
		return errors.New("profile and profile dir are required")
	}

	slog.Info("Profile set: " + profile)
	return nil
}

func (cl *ConfigLoader) loadProfileConfig() error {

	err := validateProfile(cl.profile, cl.profileDir)
	if err != nil {
		slog.Error("Profile validation error: " + err.Error())
		return err
	}

	profileConfig, err := util.LoadAndExpandYaml(cl.profileDir, "application-"+cl.profile)
	if err != nil {
		slog.Error("Error loading profile config " + err.Error())
		return err
	}

	if err := yaml.Unmarshal([]byte(profileConfig), cl.baseConfig); err != nil {
		slog.Error("Error parsing profile config " + err.Error())
		return err
	}

	return nil
}
