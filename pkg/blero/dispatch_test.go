package blero

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type noOpProcessor struct{}

func (p *noOpProcessor) Run(j *Job) error {
	return nil
}

func TestBlero_RegisterUnregisterProcessor(t *testing.T) {
	bl := New(Opts{})
	p1 := &noOpProcessor{}
	p2 := &noOpProcessor{}
	// test usage of ProcessorFunc
	p3 := ProcessorFunc(func(j *Job) error {
		return nil
	})

	pID1 := bl.RegisterProcessor(p1)
	pID2 := bl.RegisterProcessor(p2)
	pID3 := bl.RegisterProcessor(p3)
	assert.Len(t, bl.processors, 3)

	bl.UnregisterProcessor(pID2)
	assert.Len(t, bl.processors, 2)
	assert.Equal(t, p1, bl.processors[pID1])
	assert.Equal(t, nil, bl.processors[pID2])
	assert.NotNil(t, bl.processors[pID3])

	bl.UnregisterProcessor(pID3)
	assert.Len(t, bl.processors, 1)
	assert.Equal(t, p1, bl.processors[pID1])
	assert.Equal(t, nil, bl.processors[pID2])
	assert.Equal(t, nil, bl.processors[pID3])
}

type testProcessor struct {
	mock.Mock
}

func (m *testProcessor) Run(j *Job) error {
	_ = m.Called(j)

	var err error
	if j.Name == "MyOtherOtherJob" {
		err = fmt.Errorf("%v errors out", j.Name)
	}

	return err
}

func TestBlero_assignJobs(t *testing.T) {
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	j1Name := "MyJob"
	j2Name := "MyOtherJob"
	j3Name := "MyOtherOtherJob"

	p1 := new(testProcessor)
	p2 := new(testProcessor)
	p3 := new(testProcessor)

	for _, p := range []*testProcessor{p1, p2, p3} {
		p.On("Run", mock.AnythingOfType("*blero.Job"))
		bl.RegisterProcessor(p)
	}

	// test assign without jobs
	err = bl.assignJobs()
	assert.NoError(t, err)

	// enqueue jobs
	j1ID, err := bl.EnqueueJob(j1Name)
	assert.NoError(t, err)
	p1.AssertNumberOfCalls(t, "Run", 0)
	p2.AssertNumberOfCalls(t, "Run", 0)
	p3.AssertNumberOfCalls(t, "Run", 0)

	j2ID, err := bl.EnqueueJob(j2Name)
	assert.NoError(t, err)
	p1.AssertNumberOfCalls(t, "Run", 0)
	p2.AssertNumberOfCalls(t, "Run", 0)
	p3.AssertNumberOfCalls(t, "Run", 0)

	j3ID, err := bl.EnqueueJob(j3Name)
	assert.NoError(t, err)
	p1.AssertNumberOfCalls(t, "Run", 0)
	p2.AssertNumberOfCalls(t, "Run", 0)
	p3.AssertNumberOfCalls(t, "Run", 0)

	err = bl.assignJobs()
	assert.NoError(t, err)

	// wait for jobs to be processed
	time.Sleep(50 * time.Millisecond)

	p1.AssertNumberOfCalls(t, "Run", 1)
	p2.AssertNumberOfCalls(t, "Run", 1)
	p3.AssertNumberOfCalls(t, "Run", 1)

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
	assert.NoError(t, err)

	p1.AssertExpectations(t)
	p2.AssertExpectations(t)
	p3.AssertExpectations(t)
}
