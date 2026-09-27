package connect

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// defaultConfigPath is the server configuration looked up in the working
// directory when neither --config nor --timezone is given.
const defaultConfigPath = "mkts.yml"

// resolveTimezone returns the timezone the database was written with.
//
// Bucket files store rows by index from January 1 in the server's configured
// timezone, so decoding them in any other timezone shifts every timestamp by
// the zone offset. The server gets the timezone from mkts.yml; the CLI has to
// find the same value:
//
//  1. tzName (--timezone), when given.
//  2. The timezone in configPath (--config), when given. A missing or
//     unreadable file is an error, because the caller asked for it.
//  3. The timezone in ./mkts.yml, when that file exists.
//  4. UTC, with warned=true so the caller can say times may be shifted.
func resolveTimezone(configPath, tzName string) (loc *time.Location, warned bool, err error) {
	if tzName != "" {
		loc, err = time.LoadLocation(tzName)
		if err != nil {
			return nil, false, fmt.Errorf("invalid --timezone %q: %w", tzName, err)
		}
		return loc, false, nil
	}

	explicit := configPath != ""
	if !explicit {
		configPath = defaultConfigPath
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if !explicit && errors.Is(err, fs.ErrNotExist) {
			return time.UTC, true, nil
		}
		return nil, false, fmt.Errorf("read config %s: %w", configPath, err)
	}

	// Only the timezone is needed. utils.ParseConfig also validates ports,
	// plugins and the root directory, none of which matter to the CLI.
	var cfg struct {
		Timezone string `yaml:"timezone"`
	}
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, false, fmt.Errorf("parse config %s: %w", configPath, err)
	}
	// An empty timezone means the server default, which is UTC. That is a
	// correct value, not a guess, so it does not warn.
	loc, err = time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, false, fmt.Errorf("invalid timezone %q in %s: %w", cfg.Timezone, configPath, err)
	}
	return loc, false, nil
}

// configureTimezone resolves the timezone and installs it as the
// process-wide timezone used to decode and print bar times.
func configureTimezone(configPath, tzName string) error {
	loc, warned, err := resolveTimezone(configPath, tzName)
	if err != nil {
		return err
	}
	if warned {
		log.Warn("no --config, --timezone or ./%s found: decoding bar times as UTC. "+
			"If the database was written with another timezone, times will be shifted.", defaultConfigPath)
	}
	utils.InstanceConfig.Timezone = loc
	return nil
}
