package configuration

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

type LoadOptions struct {
	BaseDir    string
	ProfileEnv string
}

func DefaultOptions() LoadOptions {
	return LoadOptions{
		BaseDir:    "internal/static",
		ProfileEnv: "APP_PROFILE",
	}
}

func Load(opts ...func(*LoadOptions)) (*Properties, error) {
	o := DefaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	cfg := &Properties{}

	if err := loadFile(filepath.Join(o.BaseDir, "application.yml"), cfg); err != nil {
		return nil, fmt.Errorf("load base config: %w", err)
	}

	if envProfile := os.Getenv(o.ProfileEnv); envProfile != "" {
		cfg.App.Profile = envProfile
	}

	if cfg.App.Profile != "" {
		profilePath := filepath.Join(o.BaseDir, fmt.Sprintf("application-%s.yml", cfg.App.Profile))
		if _, err := os.Stat(profilePath); err == nil {
			if err := loadFile(profilePath, cfg); err != nil {
				return nil, fmt.Errorf("load profile config: %w", err)
			}
		}
	}

	if cfg.Raft.RaftPeersRaw != "" {
		cfg.Raft.MergePeersFromEnv()
	}

	return cfg, nil
}

func loadFile(path string, cfg *Properties) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	expanded, err := expandEnv(string(raw))
	if err != nil {
		return err
	}

	return yaml.Unmarshal([]byte(expanded), cfg)
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}`)

func expandEnv(s string) (string, error) {
	var missing []string

	result := envPattern.ReplaceAllStringFunc(s, func(match string) string {
		parts := envPattern.FindStringSubmatch(match)
		name, defaultVal := parts[1], parts[2]

		if val, ok := os.LookupEnv(name); ok {
			return val
		}
		if defaultVal != "" {
			return defaultVal
		}
		missing = append(missing, name)
		return match
	})

	if len(missing) > 0 {
		return "", fmt.Errorf("missing required env vars: %v", missing)
	}

	return result, nil
}
