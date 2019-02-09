package blero

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

type noOpProcessor struct{}

func (p *noOpProcessor) Run(j *Job) error {
	return nil
}

func TestBlero_RegisterUnregisterProcessor(t *testing.T) {
	bl := New(Opts{})
	p1 := &noOpProcessor{}
	p2 := &noOpProcessor{}
	p3 := &noOpProcessor{}

	pID1 := bl.RegisterProcessor(p1)
	pID2 := bl.RegisterProcessor(p2)
	pID3 := bl.RegisterProcessor(p3)
	assert.Len(t, bl.processors, 3)

	bl.UnregisterProcessor(pID2)
	assert.Len(t, bl.processors, 2)
	assert.Equal(t, p1, bl.processors[pID1])
	assert.Equal(t, nil, bl.processors[pID2])
	assert.Equal(t, p3, bl.processors[pID3])

	bl.UnregisterProcessor(pID3)
	assert.Len(t, bl.processors, 1)
	assert.Equal(t, p1, bl.processors[pID1])
	assert.Equal(t, nil, bl.processors[pID2])
	assert.Equal(t, nil, bl.processors[pID3])
}

type testProcessor struct {
	jobs []*Job
}

func (p *testProcessor) Run(j *Job) error {
	p.jobs = append(p.jobs, j)
	return nil
}

func TestBlero_assignJobs(t *testing.T) {
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	p1 := &testProcessor{}
	p2 := &testProcessor{}
	p3 := &testProcessor{}
	bl.RegisterProcessor(p1)
	bl.RegisterProcessor(ProcessorFunc(p2.Run))
	// test use of ProcessorFunc
	bl.RegisterProcessor(ProcessorFunc(func(j *Job) error {
		return errors.New("Failed as expected")
	}))

	// test assign without jobs
	err = bl.assignJobs()
	assert.NoError(t, err)

	// enqueue jobs
	j1Name := "MyJob"
	j1ID, err := bl.EnqueueJob(j1Name)
	assert.NoError(t, err)
	assert.Len(t, p1.jobs, 0)
	assert.Len(t, p2.jobs, 0)
	assert.Len(t, p3.jobs, 0)

	j2Name := "MyOtherJob"
	j2ID, err := bl.EnqueueJob(j2Name)
	assert.NoError(t, err)
	assert.Len(t, p1.jobs, 0)
	assert.Len(t, p2.jobs, 0)
	assert.Len(t, p3.jobs, 0)

	j3Name := "MyOtherJob"
	j3ID, err := bl.EnqueueJob(j3Name)
	assert.NoError(t, err)
	assert.Len(t, p1.jobs, 0)
	assert.Len(t, p2.jobs, 0)
	assert.Len(t, p3.jobs, 0)

	err = bl.assignJobs()
	assert.NoError(t, err)

	// wait for jobs to be processed
	time.Sleep(200 * time.Millisecond)

	assert.Len(t, p1.jobs, 1)
	assert.Equal(t, j1Name, p1.jobs[0].Name)
	assert.Equal(t, j1ID, p1.jobs[0].ID)

	assert.Len(t, p2.jobs, 1)
	assert.Equal(t, j2Name, p2.jobs[0].Name)
	assert.Equal(t, j2ID, p2.jobs[0].ID)

	err = bl.db.View(func(txn *badger.Txn) error {
		// check that job 1 is in the complete queue
		_, err := txn.Get([]byte("q:complete:" + strconv.Itoa(int(j1ID))))
		assert.NoError(t, err)

		// check that job 2 is in the complete queue
		_, err = txn.Get([]byte("q:complete:" + strconv.Itoa(int(j2ID))))
		assert.NoError(t, err)

		// check that job 3 is in the failed queue
		_, err = txn.Get([]byte("q:failed:" + strconv.Itoa(int(j3ID))))
		assert.NoError(t, err)

		return nil
	})

}
