package blero

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobStatus_String(t *testing.T) {
	assert.Equal(t, "pending", JobPending.String())
	assert.Equal(t, "inprogress", JobInProgress.String())
	assert.Equal(t, "complete", JobComplete.String())
	assert.Equal(t, "failed", JobFailed.String())

	// unknown
	assert.Equal(t, "JobStatus(50)", JobStatus(50).String())
}
