package plugins

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type SymbolLoader struct {
	module *plugin.Plugin
}

// NewSymbolLoader creates a SymbolLoader that loads symbol from a particular module.
// moduleName can be a file name under one of $GOPATH directories or current working
// directory, or an absolute path to the file.
func NewSymbolLoader(moduleName string) (*SymbolLoader, error) {
	pi, err := Load(moduleName)
	if err != nil {
		return nil, err
	}
	return &SymbolLoader{
		module: pi,
	}, nil
}

// LoadSymbol looks up a symbol from the module.  Plugin packages can accept this
// by defining an interface type without importing this package.  It is important
// to note that each plugin package cannot import this plugins package since
// plugin module cannot import any packages that import built-in plugin package.
func (l *SymbolLoader) LoadSymbol(symbolName string) (interface{}, error) {
	return l.module.Lookup(symbolName)
}

// Load loads plugin module.  If pluginName is relative path name, it is
// loaded from one of the current GOPATH directories or current working directory.
// If the path is an absolute path, it loads from the path. err is nil
// if it succeeds.
func Load(pluginName string) (*plugin.Plugin, error) {
	if filepath.IsAbs(pluginName) {
		pi, err := plugin.Open(pluginName)
		if err != nil {
			return nil, fmt.Errorf("open plugin %s: %w", pluginName, err)
		}
		return pi, nil
	}

	envGOPATH := os.Getenv("GOPATH")
	if envGOPATH == "" {
		// Use default GOPATH when not set (Go 1.8+)
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("GOPATH is not set and cannot determine home directory: %w", err)
		}
		envGOPATH = filepath.Join(homeDir, "go")
	}

	gopaths := strings.Split(envGOPATH, ":")
	candidates := make([]string, 0, len(gopaths)+1)
	for _, path := range gopaths {
		candidates = append(candidates, filepath.Join(path, "bin", pluginName))
	}
	// The working directory is checked last - helpful for testing.
	candidates = append(candidates, filepath.Join(".", pluginName))

	// Every attempt's error is retained. The GOPATH attempt is normally the
	// informative one: a plugin built against different package versions or
	// build tags than the host fails with a specific message naming the
	// package. The working-directory attempt then fails with a bare "realpath
	// failed" simply because nothing is there. Reporting only the last error
	// hides the actual cause behind that, and logging the earlier ones at
	// debug means they are invisible at the default level.
	errs := make([]error, 0, len(candidates))
	for _, pluginPath := range candidates {
		log.Info("Trying to load module from path: %s...\n", pluginPath)
		pi, err := plugin.Open(pluginPath)
		if err == nil {
			log.Info("Success loading module %s.\n", pluginPath)
			return pi, nil
		}
		log.Warn("failed to load module from %s: %v", pluginPath, err)
		errs = append(errs, fmt.Errorf("%s: %w", pluginPath, err))
	}

	return nil, fmt.Errorf("module %s not found in bin under any path in GOPATH=%s or the working directory: %w",
		pluginName, envGOPATH, errors.Join(errs...))
}
