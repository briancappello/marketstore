package connect

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/cmd/connect/session"
	"github.com/alpacahq/marketstore/v4/frontend/client"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// Command
	// -------------.
	usage   = "connect"
	short   = "Open an interactive session with an existing marketstore database"
	long    = "This command opens an interactive session with an existing marketstore database"
	example = "marketstore connect --url <address>"

	// Flags.
	// -------------
	// Network Address.
	urlFlag    = "url"
	defaultURL = ""
	urlDesc    = "network address to database instance at \"hostname:port\" when used in remote mode"
	// Local directory.
	dirFlag           = "dir"
	defaultDir        = ""
	dirDesc           = "filesystem path of the directory containing database files when used in local mode"
	defaultVarCompOff = false
	varCompOffDesc    = "disables the compression of variable data (on by default, uses snappy)"
	configDesc        = "server configuration file to read the database timezone from " +
		"(default: ./mkts.yml when present)"
	timezoneDesc = "timezone the database was written with, e.g. America/New_York " +
		"(overrides --config)"
)

var (
	// Cmd is the connect command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		SuggestFor: []string{"mkts", "open", "conn"},
		Example:    example,
		Args:       validateArgs,
		RunE:       executeConnect,
	}

	// url set via flag for remote db address.
	url string
	// dir set via flag for local directory location.
	dir string
	// turns compression of variable data off.
	varCompOff bool
	// configPath and timezone select the timezone bar times are decoded in.
	configPath string
	timezone   string
)

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	Cmd.Flags().StringVarP(&url, urlFlag, "u", defaultURL, urlDesc)
	Cmd.Flags().StringVarP(&dir, dirFlag, "d", defaultDir, dirDesc)
	Cmd.Flags().BoolVarP(&varCompOff, "disable_variable_compression", "c", defaultVarCompOff, varCompOffDesc)
	Cmd.Flags().StringVar(&configPath, "config", "", configDesc)
	Cmd.Flags().StringVar(&timezone, "timezone", "", timezoneDesc)
}

// validateArgs returns an error that prevents cmd execution if
// the custom validation fails.
func validateArgs(cmd *cobra.Command, args []string) error {
	if dir != "" && url != "" {
		return errors.New("cannot connect to database, use a flag to set location")
	}
	return nil
}

// executeConnect implements the connect command.
func executeConnect(cmd *cobra.Command, args []string) error {
	var (
		c    *session.Client
		conn session.APIClient
		err  error
	)

	// Set the timezone before any bucket is opened: local mode decodes row
	// indexes with it, and both modes print times in it, so local and remote
	// sessions started with the same flags show the same timestamps.
	if err = configureTimezone(configPath, timezone); err != nil {
		return err
	}

	// Attempt local mode.
	if dir != "" {
		conn, err = session.NewLocalAPIClient(dir)
		if err != nil {
			return err
		}
	}

	// Attempt remote mode.
	if url != "" {
		// TODO: validate url using go core packages.
		const colonSeparatedURLSliceLen = 2
		splits := strings.Split(url, ":")
		if len(splits) != colonSeparatedURLSliceLen {
			return fmt.Errorf("incorrect URL, need \"hostname:port\", have: %s", url)
		}
		// build url.
		url = "http://" + url

		// Attempt connection to remote host.
		rpcClient, err2 := client.NewClient(url)
		if err2 != nil {
			return err2
		}

		conn = session.NewRemoteAPIClient(url, rpcClient)
	}

	if varCompOff {
		utils.InstanceConfig.DisableVariableCompression = true
	}

	// Initialize connection.
	c = session.NewClient(conn)

	// Enter command loop
	err = c.Read()
	if err != nil {
		return err
	}

	log.Info("closed connection")
	return nil
}
