package util

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
