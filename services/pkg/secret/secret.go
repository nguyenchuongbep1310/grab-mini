// Package secret reads credentials the way a container platform hands them
// out, and refuses to fall back to a development default in production.
//
// A secret NAME is read from, in order:
//
//  1. the environment variable NAME_FILE, naming a file whose contents are
//     the secret (Docker and Kubernetes secrets are mounted as files, which
//     keeps them out of `docker inspect`, process listings and crash dumps);
//  2. the environment variable NAME itself;
//  3. a development default, only when APP_ENV is not "production".
//
// So the same binary runs with zero configuration on a laptop and refuses to
// start in production with the password "grabmini".
package secret

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Production reports whether APP_ENV=production.
func Production() bool { return strings.EqualFold(os.Getenv("APP_ENV"), "production") }

// Development reports whether APP_ENV=development. Unlike the other
// development defaults (which apply whenever APP_ENV is not "production"),
// key material that is public in this repository is only used when a
// developer says so explicitly, so a deployment that forgot to set APP_ENV
// fails closed instead of trusting a published key.
func Development() bool { return strings.EqualFold(os.Getenv("APP_ENV"), "development") }

// ErrMissing is returned when a secret is not set and no default is allowed.
var ErrMissing = errors.New("secret not set")

// Get returns the secret NAME, or ErrMissing (wrapped) if it is unset or empty.
func Get(name string) (string, error) {
	if path := os.Getenv(name + "_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: %w", name, err)
		}
		if v := strings.TrimRight(string(b), "\r\n"); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("%s_FILE (%s) is empty: %w", name, path, ErrMissing)
	}
	if v := os.Getenv(name); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("%s: %w", name, ErrMissing)
}

// GetOr returns the secret NAME, or devDefault when it is unset and APP_ENV is
// not "production". In production an unset secret is an error.
func GetOr(name, devDefault string) (string, error) {
	v, err := Get(name)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, ErrMissing) {
		return "", err // a named file that can't be read is a mistake, not "unset"
	}
	if Production() {
		return "", fmt.Errorf("%w (set it, or %s_FILE; development defaults are not used when APP_ENV=production)", err, name)
	}
	return devDefault, nil
}
