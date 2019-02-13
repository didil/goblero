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
}

func BenchmarkDequeue(b *testing.B) {
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

	// prepare data to be dequeued
	for n := 0; n < 20000; n++ {
		_, err := bl.EnqueueJob(jobName, jobData)
		if err != nil {
			b.Error(err)
		}
	}

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

/*
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
*/
