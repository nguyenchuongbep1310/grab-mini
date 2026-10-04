package secret

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestGetReadsTheEnvironment(t *testing.T) {
	t.Setenv("DB_PASSWORD", "hunter2")
	if v, err := Get("DB_PASSWORD"); err != nil || v != "hunter2" {
		t.Errorf("Get = %q, %v", v, err)
	}
}

func TestAFileWinsOverTheVariableAndLosesItsTrailingNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	os.WriteFile(path, []byte("from-file\n"), 0o600)
	t.Setenv("DB_PASSWORD", "from-env")
	t.Setenv("DB_PASSWORD_FILE", path)

	if v, err := Get("DB_PASSWORD"); err != nil || v != "from-file" {
		t.Errorf("Get = %q, %v; want the file's content without its newline", v, err)
	}
}

func TestAnUnreadableOrEmptyFileIsAnErrorNotADefault(t *testing.T) {
	t.Setenv("DB_PASSWORD_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := GetOr("DB_PASSWORD", "dev"); err == nil {
		t.Error("a _FILE that can't be read fell back to the default")
	}

	empty := filepath.Join(t.TempDir(), "empty")
	os.WriteFile(empty, []byte("\n"), 0o600)
	t.Setenv("DB_PASSWORD_FILE", empty)
	if _, err := Get("DB_PASSWORD"); !errors.Is(err, ErrMissing) {
		t.Errorf("an empty secret file gave %v, want ErrMissing", err)
	}
}

func TestDevelopmentDefaultsApplyOutsideProduction(t *testing.T) {
	t.Setenv("APP_ENV", "")
	if v, err := GetOr("UNSET_SECRET", "dev-value"); err != nil || v != "dev-value" {
		t.Errorf("GetOr = %q, %v; want the default", v, err)
	}
}

func TestProductionRefusesToUseADefault(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	if _, err := GetOr("UNSET_SECRET", "dev-value"); !errors.Is(err, ErrMissing) {
		t.Errorf("GetOr in production = %v, want ErrMissing", err)
	}
	t.Setenv("UNSET_SECRET", "real")
	if v, err := GetOr("UNSET_SECRET", "dev-value"); err != nil || v != "real" {
		t.Errorf("GetOr in production with the secret set = %q, %v", v, err)
	}
}
