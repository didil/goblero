package blero

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobStatus_String(t *testing.T) {
	assert.Equal(t, "pending", jobPending.String())
	assert.Equal(t, "inprogress", jobInProgress.String())
	assert.Equal(t, "complete", jobComplete.String())
	assert.Equal(t, "failed", jobFailed.String())

	// unknown
	assert.Equal(t, "jobStatus(50)", jobStatus(50).String())
}
