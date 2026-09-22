// Package trigger provides interface for trigger plugins.
// A trigger plugin has to implement the following function.
// - NewTrigger(config map[string]interface{}) (Trigger, error)
//
// The trigger instance returned by this function will be called on Fire()
// with the filePath (relative to root directory) and indexes that have been written
// (appended or updated).  It is guaranteed that the new content has been written
// on disk when Fire() is called, so it is safe to read it from disk.  Keep in mind
// that the trigger might be called on the startup, due to the WAL recovery.
//
// Triggers can be configured in the marketstore config file.
//
//	triggers:
//	  - module: xxxTrigger.so
//	    on: "*/1Min/OHLCV"
//	    config: <according to the plugin>
//
// The "on" value is matched with the file path to decide whether the trigger
// is fired or not.  It can contain wildcard character "*".
// As of now, trigger fires only on the running state.  Trigger on WAL replay
// may be added later.
package trigger

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/plugins"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Trigger is an interface every trigger plugin has to implement.
type Trigger interface {
	// Fire is called when the target file has been modified.
	// keyPath is the string path of the modified file relative
	// from the catalog root directory.  indexes is a slice
	// containing indexes of the rows being modified.
	Fire(keyPath string, records []Record)
}

// Matcher checks if the trigger should be fired or not.
type Matcher struct {
	Trigger Trigger
	// On is a string representing the condition of the trigger
	// fire event.  It is the prefix of file path such as
	// ""*/1Min/OHLC"
	On string
	// onRegex is On compiled once by NewMatcher. Match runs on the write hot
	// path -- once per registered matcher for every key written -- so
	// compiling the pattern inside Match made regex compilation a measurable
	// share of both CPU time and total heap allocation under load.
	onRegex *regexp.Regexp
}

// compileOn builds the matching expression for an "on" condition.
//
// "on" is a glob over key paths, not a regular expression: the only
// metacharacter is "*", standing for exactly one path element. Everything else
// is literal, so the pattern is quoted before "*" is substituted -- otherwise a
// "." in a bucket name would silently behave as "any character".
//
// The expression is anchored. A key path carries a trailing year file
// ("AAPL/1Min/OHLCV/2024.bin") while "on" names the bucket ("*/1Min/OHLCV"), so
// the match must start at the beginning and end on a path boundary. Without the
// anchors the old unanchored match fired "*/1Min/OHLCV" on a differently-named
// "AAPL/1Min/OHLCVEXTRA" bucket, and on any key merely containing the pattern
// somewhere in the middle.
func compileOn(on string) (*regexp.Regexp, error) {
	pattern := strings.ReplaceAll(regexp.QuoteMeta(on), `\*`, `[^/]+`)
	return regexp.Compile(`^` + pattern + `(?:/|$)`)
}

// SymbolLoader is an interface to retrieve symbol object from plugin.
type SymbolLoader interface {
	LoadSymbol(symbolName string) (interface{}, error)
}

// Record represents a serialized byte buffer
// for a record written to the DB.
type Record []byte

// Bytes returns the raw record buffer.
func (r *Record) Bytes() []byte {
	return *r
}

// Index returns the index of the record.
func (r *Record) Index() int64 {
	if r == nil {
		return 0
	}
	return io.ToInt64((*r)[0:8])
}

// Payload returns the data payload of the record,
// excluding the index.
func (r *Record) Payload() []byte {
	if r == nil {
		return nil
	}
	return (*r)[8:]
}

// RecordsToColumnSeries takes a slice of Record, along with the required
// information for constructing a ColumnSeries, and builds it from the
// slice of Record.
func RecordsToColumnSeries(
	tbk io.TimeBucketKey,
	ds []io.DataShape,
	tf time.Duration,
	year int16,
	records []Record,
) (*io.ColumnSeries, error) {
	cs := io.NewColumnSeries()

	index := 0

	for _, s := range ds {
		data := []byte{}

		for _, record := range records {
			slc := record.Bytes()[index : index+s.Len()]
			if strings.EqualFold(s.Name, "Epoch") {
				buf, _ := io.Serialize(nil,
					io.IndexToTime(io.ToInt64(slc), tf, year).Unix())
				data = append(data, buf...)
			} else {
				data = append(data, slc...)
			}
		}

		column, err := s.Type.ConvertByteSliceInto(data)
		if err != nil {
			return nil, fmt.Errorf("convert buffer to column records: %w", err)
		}
		cs.AddColumn(s.Name, column)
		index += s.Len()
	}

	return cs, nil
}

// Load loads a function named NewTrigger with a parameter type map[string]interface{}
// and initialize the trigger.
func Load(loader SymbolLoader, config map[string]interface{}) (Trigger, error) {
	symbolName := "NewTrigger"
	sym, err := loader.LoadSymbol(symbolName)
	if err != nil {
		return nil, fmt.Errorf("unable to load %s", symbolName)
	}

	newFunc, ok := sym.(func(map[string]interface{}) (Trigger, error))
	if !ok {
		return nil, fmt.Errorf("%s does not comply function spec", symbolName)
	}
	return newFunc(config)
}

func NewTriggerMatchers(triggers []*utils.TriggerSetting) []*Matcher {
	log.Info("InitializeTriggers")
	var triggerMatchers []*Matcher

	for _, triggerSetting := range triggers {
		log.Info("triggerSetting = %v", triggerSetting)
		tmatcher := NewTriggerMatcher(triggerSetting)
		if tmatcher != nil {
			triggerMatchers = append(
				triggerMatchers, tmatcher)
		}
	}
	log.Info("InitializeTriggers - Done")
	return triggerMatchers
}

func NewTriggerMatcher(ts *utils.TriggerSetting) *Matcher {
	loader, err := plugins.NewSymbolLoader(ts.Module)
	if err != nil {
		log.Error("Unable to open plugin for trigger in %s: %v", ts.Module, err)
		return nil
	}
	trig, err := Load(loader, ts.Config)
	if err != nil {
		log.Error("Error returned while creating a trigger: %v", err)
		return nil
	}
	return NewMatcher(trig, ts.On)
}

// NewMatcher creates a new Matcher.
func NewMatcher(trigger Trigger, on string) *Matcher {
	re, err := compileOn(on)
	if err != nil {
		// A pattern that cannot compile would never match anything, so fail
		// loudly here rather than silently never firing the trigger at runtime.
		log.Error("invalid trigger 'on' condition %q: %v", on, err)
		return nil
	}
	return &Matcher{
		Trigger: trigger, On: on, onRegex: re,
	}
}

// Match returns true if keyPath matches the On condition.
func (tm *Matcher) Match(keyPath string) bool {
	if tm.onRegex == nil {
		// Only reachable for a Matcher built as a bare struct literal instead
		// of via NewMatcher. Compile per call rather than caching, to keep
		// Match safe for concurrent use.
		re, err := compileOn(tm.On)
		if err != nil {
			return false
		}
		return re.MatchString(keyPath)
	}
	return tm.onRegex.MatchString(keyPath)
}
