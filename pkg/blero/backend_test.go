package blero

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlero_StartNoDBPath(t *testing.T) {
	bl := New("")
	err := bl.Start()
	assert.EqualError(t, err, "DBPath is required")
}
