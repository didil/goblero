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

func TestBlero_StartDBPathDoesntExist(t *testing.T) {
	bl := New("/tmp/12354/56547/45459/blero/dbx/")
	err := bl.Start()
	assert.EqualError(t, err, "Error Creating Dir: \"/tmp/12354/56547/45459/blero/dbx/\": mkdir /tmp/12354/56547/45459/blero/dbx/: no such file or directory")
}

func BenchmarkEnqueue(b *testing.B) {
	bl := New(testDBPath)
	err := bl.Start()
	if err != nil {
		b.Error(err)
	}

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	jobName := "MyJob"
	jobData := []byte("MyJobData")
	b.ResetTimer()

	b.Run("EnqueueJob", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := bl.EnqueueJob(jobName, jobData)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.ResetTimer()

	b.Run("dequeueJob", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := bl.queue.dequeueJob()
			if err != nil {
				b.Error(err)
			}
		}
	})

}
