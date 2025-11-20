package initializer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeYAMLDir(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name+".yml")
	err := os.WriteFile(path, []byte(content), 0777)
	require.NoError(t, err, "failed to write yaml %s", path)
}

func newTestDirs(t *testing.T) (baseDir, profileDir string) {
	t.Helper()
	return t.TempDir(), t.TempDir()
}

func TestInitialize_success(t *testing.T) {
	baseDir, profileDir := newTestDirs(t)
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "meta:\n  profile: \"local\"")
	writeYAMLDir(t, profileDir, "application-local", "")

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "local", cfg.Meta.Profile)

}

func TestInitialize_missingProfile(t *testing.T) {
	baseDir, profileDir := newTestDirs(t)
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "")

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "profile")
}

func TestInitialize_missingBaseFile(t *testing.T) {
	baseDir, profileDir := newTestDirs(t)
	logLevel := "debug"

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "application")
}

func TestInitialize_missingDir(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := filepath.Join(baseDir, "profiles")
	logLevel := "debug"

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "application.yml not found")

}

func TestInitialize_invalidYaml(t *testing.T) {
	baseDir, profileDir := newTestDirs(t)
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "meta:\n  profile: \"test\"")
	writeYAMLDir(t, profileDir, "application-test", "foo \"bar\"\nfoo:: \"bar\"\nfoo: \"bar\"")

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "mapping values are not allowed in this context")

}

func TestInitialize_missingProfileConfig(t *testing.T) {
	baseDir, profileDir := newTestDirs(t)
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "meta:\n")
	writeYAMLDir(t, profileDir, "application-local", "")

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "profile")
}

func TestInitialize_missingProfileFile(t *testing.T) {
	baseDir, profileDir := newTestDirs(t)
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "meta:\n  profile: \"test\"")
	writeYAMLDir(t, profileDir, "application-local", "")

	cfg, err := Initialize(baseDir, profileDir, logLevel)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "application-test")
}
