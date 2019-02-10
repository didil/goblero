package blero

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlero_StartNoDBPath(t *testing.T) {
	bl := New(Opts{DBPath: ""})
	err := bl.Start()
	assert.EqualError(t, err, "Opts: DBPath is required")
}

type testLogger struct{}

func (l *testLogger) Errorf(string, ...interface{})       {}
func (l *testLogger) Infof(s string, rest ...interface{}) {}
func (l *testLogger) Warningf(string, ...interface{})     {}

func TestBlero_StartCustomLogger(t *testing.T) {
	logger := &testLogger{}

	bl := New(Opts{
		DBPath: testDBPath,
		Logger: logger,
	})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()
}
