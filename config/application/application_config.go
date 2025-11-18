package applicationConfig

import (
	"fmt"
	"log/slog"

	"os"
	"path/filepath"
	"pulsardb/config/application/properties"
	"regexp"

	"gopkg.in/yaml.v3"
)

func Initialize(baseName string, baseDir string, profileDir string) *properties.Config {
	baseConfig, err := loadAndExpandYaml(baseDir, baseName)
	if err != nil {
		slog.Error("Error loading base config", err.Error())
		os.Exit(-1)
	}

	var cfg properties.Config
	if err := yaml.Unmarshal([]byte(baseConfig), &cfg); err != nil {
		slog.Error("Error parsing base config", err.Error())
		os.Exit(-1)
	}

	profile := cfg.Meta.Profile

	if profile == "" || profileDir == "" {
		slog.Error("profile and profile dir are required")
		os.Exit(-1)
	}

	profileConfig, err := loadAndExpandYaml(profileDir, baseName+"-"+profile)
	if err != nil {
		slog.Error("Error loading profile config", err.Error())
		os.Exit(-1)
	}

	if err := yaml.Unmarshal([]byte(profileConfig), &cfg); err != nil {
		slog.Error("Error parsing profile config", err.Error())
		os.Exit(-1)
	}

	return &cfg
}

func loadAndExpandYaml(dir, name string) (string, error) {
	file := filepath.Join(dir, name+".yml")
	if _, err := os.Stat(file); err != nil {
		return "", fmt.Errorf("%s.yml not found", name)
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
