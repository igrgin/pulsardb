package applicationConfig

import (
	"fmt"
	"os"
	"path/filepath"
	"pulsardb/application-config/properties"
	"regexp"

	"gopkg.in/yaml.v3"
)

func LoadConfig() (*properties.Config, error) {
	baseConfig, err := loadAndExpandYaml("application-config", "application")
	if err != nil {
		return nil, err
	}

	var cfg properties.Config
	if err := yaml.Unmarshal([]byte(baseConfig), &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal base config: %w", err)
	}

	profileDir := cfg.Meta.ProfilesDir
	profile := cfg.Meta.Profile

	if profile == "" || profileDir == "" {
		return nil, fmt.Errorf("profile or profiles_dir not set")
	}

	profileConfig, err := loadAndExpandYaml(profileDir, "application-"+profile)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal([]byte(profileConfig), &cfg); err != nil {
		return nil, fmt.Errorf("Unmarshal profile config: %w", err)
	}

	return &cfg, nil
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
