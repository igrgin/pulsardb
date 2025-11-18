package applicationConfig

import (
	"os"
	"path/filepath"
	"testing"
)

func writeYAMLDir(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name+".yml")
	if err := os.WriteFile(path, []byte(content), 0777); err != nil {
		t.Fatalf("failed to write yaml %s: %v", path, err)
	}
}

func TestExpandEnvStrict_MissingEnv(t *testing.T) {
	s := "hello ${FOO} world"
	_, err := ExpandEnvStrict(s)
	if err == nil {
		t.Fatal("expected error, got none")
	}
}

func TestExpandEnvStrict_InvalidEnv(t *testing.T) {
	os.Setenv("FOO", "bar")
	defer os.Unsetenv("FOO")

	s := "hello ${FOO bar world} world"
	_, err := ExpandEnvStrict(s)
	if err == nil {
		t.Fatal("expected error, got none")
	}
	os.Unsetenv("FOO")
}

func TestExpandEnvStrict_Success(t *testing.T) {
	os.Setenv("FOO", "bar")
	defer os.Unsetenv("FOO")

	s := "hello ${FOO} world"
	got, err := ExpandEnvStrict(s)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	want := "hello bar world"
	if got != want {
		t.Errorf("got %q; want %q", got, want)
	}
}

func TestLoadConfig_success(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := t.TempDir()
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "meta:\n  profile: \"local\"")
	writeYAMLDir(t, profileDir, "application-local", "")

	cfg, _ := Initialize(baseDir, profileDir, logLevel)

	t.Logf("%+v", cfg)
}

func TestLoadConfig_missingProfile(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := t.TempDir()
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application", "")

	_, _ = Initialize(baseDir, profileDir, logLevel)
}

func TestLoadConfig_missingFile(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := t.TempDir()
	logLevel := "debug"

	_, err := Initialize(baseDir, profileDir, logLevel)
	if err == nil {
		t.Fatal("expected error, got none")
	}

}

func TestLoadConfig_missingDir(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := filepath.Join(baseDir, "profiles")
	logLevel := "debug"

	_, err := Initialize(baseDir, profileDir, logLevel)
	if err == nil {
		t.Fatal("expected error, got none")
	}

}

func TestLoadConfig_invalidYaml(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := t.TempDir()
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application-test", "meta:\n  profile: \"local\"")
	writeYAMLDir(t, profileDir, "application-local", "foo \"bar\"\nfoo:: \"bar\"\nfoo: \"bar\"")

	_, err := Initialize(baseDir, profileDir, logLevel)
	if err == nil {
		t.Fatal("expected error, got none")
	}

}

func TestLoadConfig_invalidProfile(t *testing.T) {
	baseDir := t.TempDir()
	profileDir := t.TempDir()
	logLevel := "debug"

	writeYAMLDir(t, baseDir, "application-test", "meta:\n  profile: \"test\"")
	writeYAMLDir(t, profileDir, "application-local", "")

	_, err := Initialize(baseDir, profileDir, logLevel)
	if err == nil {
		t.Fatal("expected error, got none")
	}
}
