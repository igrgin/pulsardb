package util

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"pulsardb/internal/configuration/properties"
	"regexp"

	"gopkg.in/yaml.v3"
)

func LoadAndExpandYaml(baseDir, filename string) (string, error) {
	file := filepath.Join(baseDir, filename+".yml")
	if _, err := os.Stat(file); err != nil {
		return "", fmt.Errorf("%s.yml not found", filename)
	}

	raw, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}

	expanded, err := ExpandEnvStrict(string(raw))
	if err != nil {
		return "", err
	}

	return expanded, nil
}

func ExpandEnvStrict(s string) (string, error) {
	re := regexp.MustCompile(`\${([^}]+)}`)

	matches := re.FindAllStringSubmatch(s, -1)
	for _, m := range matches {
		name := m[1]
		if _, ok := os.LookupEnv(name); !ok {
			return "", fmt.Errorf("environment variable %s is not set", name)
		}
	}

	return os.ExpandEnv(s), nil
}

func LoadBaseConfig() (*properties.Config, error) {
	baseConfig, err := LoadAndExpandYaml("internal/static", "application")
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

func LoadProfileConfig(cfg *properties.Config) error {

	profileConfig, err := LoadAndExpandYaml("internal/static", "application-"+cfg.Application.Profile)
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
