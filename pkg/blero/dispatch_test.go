package blero

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"runtime"
	"sync"
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
	bl := New(testDBPath)
	p1 := &noOpProcessor{}
	p2 := &noOpProcessor{}
	// test usage of ProcessorFunc
	p3 := func(j *Job) error {
		return nil
	}

	d := bl.dispatcher
	pStore := d.pStore

	pID1 := bl.RegisterProcessor(p1)
	pID2 := bl.RegisterProcessor(p2)
	pID3 := bl.RegisterProcessorFunc(p3)
	assert.Len(t, pStore.processors, 3)
	assert.Equal(t, 3, pStore.maxProcessorID)

	bl.UnregisterProcessor(pID2)
	assert.Len(t, pStore.processors, 2)
	assert.Equal(t, p1, pStore.processors[pID1])
	assert.Equal(t, nil, pStore.processors[pID2])
	assert.NotNil(t, pStore.processors[pID3])

	bl.UnregisterProcessor(pID3)
	assert.Len(t, pStore.processors, 1)
	assert.Equal(t, p1, pStore.processors[pID1])
	assert.Equal(t, nil, pStore.processors[pID2])
	assert.Equal(t, nil, pStore.processors[pID3])
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
	bl := New(testDBPath)
	// only start the queue and not the dispatch loop to allow manual jobs assignment
	err := bl.queue.start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	d := bl.dispatcher
	q := bl.queue

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
	err = d.assignJobs(q)
	assert.NoError(t, err)

	// enqueue jobs
	j1ID, err := bl.EnqueueJob(j1Name, nil)
	assert.NoError(t, err)
	p1.AssertNumberOfCalls(t, "Run", 0)
	p2.AssertNumberOfCalls(t, "Run", 0)
	p3.AssertNumberOfCalls(t, "Run", 0)

	j2ID, err := bl.EnqueueJob(j2Name, nil)
	assert.NoError(t, err)
	p1.AssertNumberOfCalls(t, "Run", 0)
	p2.AssertNumberOfCalls(t, "Run", 0)
	p3.AssertNumberOfCalls(t, "Run", 0)

	j3ID, err := bl.EnqueueJob(j3Name, nil)
	assert.NoError(t, err)
	p1.AssertNumberOfCalls(t, "Run", 0)
	p2.AssertNumberOfCalls(t, "Run", 0)
	p3.AssertNumberOfCalls(t, "Run", 0)

	err = d.assignJobs(q)
	assert.NoError(t, err)

	// wait for jobs to be processed
	time.Sleep(50 * time.Millisecond)

	p1.AssertNumberOfCalls(t, "Run", 1)
	p2.AssertNumberOfCalls(t, "Run", 1)
	p3.AssertNumberOfCalls(t, "Run", 1)

	err = q.db.View(func(txn *badger.Txn) error {
		// check that job 1 is in the complete queue
		_, err := txn.Get([]byte("q:complete:" + jIDString(j1ID)))
		assert.NoError(t, err)

		// check that job 2 is in the complete queue
		_, err = txn.Get([]byte("q:complete:" + jIDString(j2ID)))
		assert.NoError(t, err)

		// check that job 3 is in the failed queue
		_, err = txn.Get([]byte("q:failed:" + jIDString(j3ID)))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	p1.AssertExpectations(t)
	p2.AssertExpectations(t)
	p3.AssertExpectations(t)
}

func TestBlero_AutoProcessing_ProcessorFirst(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	var m sync.Mutex
	var calls []string

	bl.RegisterProcessor(ProcessorFunc(func(j *Job) error {
		m.Lock()
		calls = append(calls, j.Name)
		m.Unlock()
		return nil
	}))

	// simulate wait period
	time.Sleep(50 * time.Millisecond)

	j1Name := "MyJob"
	j2Name := "MyOtherJob"

	bl.EnqueueJob(j1Name, nil)
	bl.EnqueueJob(j2Name, nil)

	// wait for jobs to be processed
	time.Sleep(50 * time.Millisecond)

	m.Lock()
	assert.Len(t, calls, 2)
	assert.ElementsMatch(t, []string{j1Name, j2Name}, calls)
	m.Unlock()
}

func TestBlero_AutoProcessing_JobsFirst(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	var m sync.Mutex
	var calls []string

	j1Name := "MyJob"
	j2Name := "MyOtherJob"

	bl.EnqueueJob(j1Name, nil)
	bl.EnqueueJob(j2Name, nil)

	// simulate wait period
	time.Sleep(50 * time.Millisecond)

	bl.RegisterProcessor(ProcessorFunc(func(j *Job) error {
		m.Lock()
		calls = append(calls, j.Name)
		m.Unlock()
		return nil
	}))

	// wait for jobs to be processed
	time.Sleep(50 * time.Millisecond)

	m.Lock()
	assert.Len(t, calls, 2)
	assert.ElementsMatch(t, []string{j1Name, j2Name}, calls)
	m.Unlock()
}

func TestBlero_AutoProcessing_GoRoutinesHanging(t *testing.T) {
	bl := New(testDBPath)
	// only start the queue and not the dispatch loop to not run assign and show goroutines hanging problem
	err := bl.queue.start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)

	for index := 0; index < 300; index++ {
		bl.EnqueueJob("FakeJob", nil)
	}

	bl.Stop()

	// simulate wait period
	time.Sleep(50 * time.Millisecond)

	assert.True(t, runtime.NumGoroutine() < 20)
}

type safeBuffer struct {
	bytes.Buffer
	m sync.Mutex
}

func (b *safeBuffer) Read(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.Buffer.Read(p)
}

func (b *safeBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.Buffer.Write(p)
}

func Test_dispatcherAssignFails(t *testing.T) {
	stdErr = new(safeBuffer)
	pStore := newProcessorsStore()
	// introduce error by registering nil processor
	pStore.registerProcessor(nil)
	d := newDispatcher(pStore)

	d.startLoop(nil)

	// send assign signal
	d.signalLoop()

	time.Sleep(50 * time.Millisecond)
	errText, err := ioutil.ReadAll(stdErr)
	assert.NoError(t, err)

	assert.Equal(t, "Cannot assign jobs: Processor 1 not found", string(errText))
}
