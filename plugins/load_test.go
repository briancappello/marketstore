package plugins_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/plugins"
)

func setup(t *testing.T) (tearDown func(), testPluginLib, oldGoPath, absTestPluginLib string) {
	t.Helper()

	dirName := t.TempDir()

	if osType := runtime.GOOS; osType != "linux" {
		t.Skip("Only linux runs plugins")
	}

	binDirName := filepath.Join(dirName, "bin")
	assert.Nil(t, os.MkdirAll(binDirName, 0o777))
	testFileName := "plugin.go"
	testFilePath := filepath.Join(dirName, testFileName)
	testPluginLib = "plugin.so"
	soFilePath := filepath.Join(binDirName, testPluginLib)
	file, err := os.Create(testFilePath)
	if err != nil {
		t.Fatal("Could not create test plugin source file")
	}
	code := `package main
func main() {}
`
	_, err = file.WriteString(code)
	assert.Nil(t, err)
	assert.Nil(t, file.Close())
	cmd := exec.Command("go",
		"build",
		"-buildmode=plugin",
		"-o",
		soFilePath,
		testFilePath)

	if err := cmd.Run(); err != nil {
		t.Log(err)
		t.Skip("Unable to build test plugin ** is go version > 1.9 in your path?")
	}

	goPath := os.Getenv("GOPATH")
	newGoPath := dirName + ":" + goPath
	oldGoPath = goPath
	absTestPluginLib = soFilePath
	os.Setenv("GOPATH", newGoPath)

	return func() {
		if oldGoPath != "" {
			os.Setenv("GOPATH", oldGoPath)
		}
	}, testPluginLib, oldGoPath, absTestPluginLib
}

func TestLoadFromGOPATH(t *testing.T) {
	tearDown, testPluginLib, _, absTestPluginLib := setup(t)
	defer tearDown()

	var pi *plugin.Plugin
	var err error
	pi, err = plugins.Load(testPluginLib)
	assert.NotNil(t, pi)
	assert.Nil(t, err)

	pi, err = plugins.Load("nonexistent")
	assert.Nil(t, pi)
	assert.NotNil(t, err)

	// abs path case
	pi, err = plugins.Load(absTestPluginLib)
	assert.NotNil(t, pi)
	assert.Nil(t, err)
}

// TestLoadErrorReportsGOPATHAttempt covers the case where the module exists in
// GOPATH/bin but cannot be opened -- in practice a plugin built against
// different package versions or build tags than the host binary.
//
// Load falls back to the working directory, where nothing is present, so that
// attempt fails with a bare "realpath failed". Previously only that last error
// was returned and the real GOPATH error was logged at debug, so the reported
// cause was actively misleading. The error must name the GOPATH path it tried.
func TestLoadErrorReportsGOPATHAttempt(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Only linux runs plugins")
	}

	dirName := t.TempDir()
	binDirName := filepath.Join(dirName, "bin")
	assert.Nil(t, os.MkdirAll(binDirName, 0o777))

	// A file that exists and is named like a plugin, but is not one.
	brokenSo := filepath.Join(binDirName, "broken.so")
	assert.Nil(t, os.WriteFile(brokenSo, []byte("not an elf shared object"), 0o600))

	oldGoPath := os.Getenv("GOPATH")
	t.Cleanup(func() { os.Setenv("GOPATH", oldGoPath) })
	os.Setenv("GOPATH", dirName)

	pi, err := plugins.Load("broken.so")
	assert.Nil(t, pi)
	assert.NotNil(t, err)

	// The GOPATH candidate must be named, along with the module and GOPATH.
	assert.Contains(t, err.Error(), brokenSo,
		"error must name the GOPATH path that actually failed, not just the cwd fallback")
	assert.Contains(t, err.Error(), "broken.so")
	assert.Contains(t, err.Error(), dirName)
}

// TestLoadErrorReportsEveryCandidate pins that all candidate paths are
// reported, so a multi-entry GOPATH does not hide whichever one the operator
// actually meant.
func TestLoadErrorReportsEveryCandidate(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Only linux runs plugins")
	}

	first := t.TempDir()
	second := t.TempDir()

	oldGoPath := os.Getenv("GOPATH")
	t.Cleanup(func() { os.Setenv("GOPATH", oldGoPath) })
	os.Setenv("GOPATH", first+":"+second)

	_, err := plugins.Load("absent.so")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), filepath.Join(first, "bin", "absent.so"))
	assert.Contains(t, err.Error(), filepath.Join(second, "bin", "absent.so"))
}
