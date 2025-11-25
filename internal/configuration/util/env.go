package util

import (
	"fmt"
	"os"
	"regexp"
)

var envVarPattern = regexp.MustCompile(`\${([^}]+)}`)

func ExpandEnvStrict(s string) (string, error) {
	matches := envVarPattern.FindAllStringSubmatch(s, -1)
	for _, m := range matches {
		name := m[1]
		if _, ok := os.LookupEnv(name); !ok {
			return "", fmt.Errorf("environment variable %s is not set", name)
		}
	}

	return os.ExpandEnv(s), nil
}
