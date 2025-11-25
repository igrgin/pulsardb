package util

import (
	"fmt"
	"os"
	"path/filepath"
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
