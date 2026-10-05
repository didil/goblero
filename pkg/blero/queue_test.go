package blero

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDBPath = "../../db/test"

func TestBlero_StartRestoresInterruptedJobs(t *testing.T) {
	dbPath := t.TempDir()
	q := newQueue(queueOpts{DBPath: dbPath})
	require.NoError(t, q.start())

	jobs := make([]*Job, 5)
	for i := range jobs {
		name := fmt.Sprintf("job-%d", i)
		data := []byte{byte(i), 0, 255}
		id, err := q.enqueueJob(name, data)
		require.NoError(t, err)
		jobs[i] = &Job{ID: id, Name: name, Data: data}
	}
	for _, status := range []jobStatus{jobComplete, jobFailed} {
		j, err := q.dequeueJob()
		require.NoError(t, err)
		require.NoError(t, q.markJobDone(j.ID, status))
	}
	for i := 2; i < 4; i++ {
		_, err := q.dequeueJob()
		require.NoError(t, err)
	}
	require.NoError(t, q.stop())

	q = newQueue(queueOpts{DBPath: dbPath})
	require.NoError(t, q.start())
	defer func() { require.NoError(t, q.stop()) }()

	require.NoError(t, q.db.View(func(txn *badger.Txn) error {
		for i, status := range []jobStatus{jobComplete, jobFailed, jobPending, jobPending, jobPending} {
			j, err := getJobForKey(txn, []byte(getJobKey(status, jobs[i].ID)))
			require.NoError(t, err)
			assert.Equal(t, jobs[i], j)
			_, err = txn.Get([]byte(getJobKey(jobInProgress, jobs[i].ID)))
			assert.ErrorIs(t, err, badger.ErrKeyNotFound)
		}
		return nil
	}))

	for _, expected := range jobs[2:] {
		j, err := q.dequeueJob()
		require.NoError(t, err)
		assert.Equal(t, expected, j)
		require.NoError(t, q.markJobDone(j.ID, jobComplete))
	}
	j, err := q.dequeueJob()
	require.NoError(t, err)
	assert.Nil(t, j)
}

func TestBlero_StartDispatchesInterruptedJobs(t *testing.T) {
	dbPath := t.TempDir()
	q := newQueue(queueOpts{DBPath: dbPath})
	require.NoError(t, q.start())
	id, err := q.enqueueJob("interrupted", []byte("payload"))
	require.NoError(t, err)
	_, err = q.dequeueJob()
	require.NoError(t, err)
	require.NoError(t, q.stop())

	bl := New(dbPath)
	processed := make(chan *Job, 1)
	bl.RegisterProcessorFunc(func(j *Job) error {
		processed <- j
		return nil
	})
	require.NoError(t, bl.Start())
	defer func() { require.NoError(t, bl.Stop()) }()

	select {
	case j := <-processed:
		assert.Equal(t, &Job{ID: id, Name: "interrupted", Data: []byte("payload")}, j)
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted job was not dispatched after startup")
	}
	require.Eventually(t, func() bool {
		return bl.queue.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(getJobKey(jobComplete, id)))
			return err
		}) == nil
	}, 5*time.Second, time.Millisecond)
}

func TestBlero_StartRestoresJobsAfterCrash(t *testing.T) {
	if dbPath := os.Getenv("GOBLERO_TEST_CRASH_DB"); dbPath != "" {
		q := newQueue(queueOpts{DBPath: dbPath})
		require.NoError(t, q.start())
		for i := 0; i < 3; i++ {
			_, err := q.enqueueJob("crash", []byte{byte(i), 0, 255})
			require.NoError(t, err)
		}
		for i := 0; i < 2; i++ {
			_, err := q.dequeueJob()
			require.NoError(t, err)
		}
		os.Exit(0)
	}

	dbPath := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestBlero_StartRestoresJobsAfterCrash$")
	cmd.Env = append(os.Environ(), "GOBLERO_TEST_CRASH_DB="+dbPath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)

	q := newQueue(queueOpts{DBPath: dbPath})
	require.NoError(t, q.start())
	defer func() { require.NoError(t, q.stop()) }()
	id, err := q.enqueueJob("new", []byte("new payload"))
	require.NoError(t, err)
	assert.Greater(t, id, uint64(3))
	require.NoError(t, q.stop())

	q = newQueue(queueOpts{DBPath: dbPath})
	require.NoError(t, q.start())
	for i := 0; i < 3; i++ {
		j, err := q.dequeueJob()
		require.NoError(t, err)
		require.Equal(t, &Job{ID: uint64(i + 1), Name: "crash", Data: []byte{byte(i), 0, 255}}, j)
		require.NoError(t, q.markJobDone(j.ID, jobComplete))
	}
	j, err := q.dequeueJob()
	require.NoError(t, err)
	require.Equal(t, &Job{ID: id, Name: "new", Data: []byte("new payload")}, j)
	require.NoError(t, q.markJobDone(j.ID, jobComplete))
	j, err = q.dequeueJob()
	require.NoError(t, err)
	assert.Nil(t, j)
}

func TestBlero_RestoreInterruptedJobsBeyondTransactionLimit(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil).
		WithMemTableSize(1 << 20).WithValueThreshold(32 << 10)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	q := &queue{db: db}
	jobs := make([]*Job, 32)
	for i := range jobs {
		jobs[i] = &Job{ID: uint64(i + 1), Name: "large", Data: bytes.Repeat([]byte{byte(i)}, 16<<10)}
		b, err := encodeJob(jobs[i])
		require.NoError(t, err)
		require.NoError(t, db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(getJobKey(jobInProgress, jobs[i].ID)), b)
		}))
	}
	err = db.Update(func(txn *badger.Txn) error {
		for _, j := range jobs {
			k := []byte(getJobKey(jobInProgress, j.ID))
			v, err := getBytesForKey(txn, k)
			if err != nil {
				return err
			}
			if err := moveItem(txn, k, []byte(getJobKey(jobPending, j.ID)), v); err != nil {
				return err
			}
		}
		return nil
	})
	require.ErrorIs(t, err, badger.ErrTxnTooBig)

	require.NoError(t, q.restoreInterruptedJobs())
	for _, expected := range jobs {
		j, err := q.dequeueJob()
		require.NoError(t, err)
		require.Equal(t, expected, j)
		require.NoError(t, q.markJobDone(j.ID, jobComplete))
	}
	j, err := q.dequeueJob()
	require.NoError(t, err)
	assert.Nil(t, j)
}

func deleteDBFolder(dbPath string) {
	// prevent accidental deletion of non badgerdb folder
	if _, err := os.Stat(filepath.Join(dbPath, "MANIFEST")); os.IsNotExist(err) {
		panic("Attempted to delete non badgerdb folder " + dbPath)
	}

	err := os.RemoveAll(dbPath)
	if err != nil {
		panic(err)
	}
}

/*
func TestBlero_StopQueueAlreadyStopped(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// delete folder
	defer deleteDBFolder(testDBPath)
	bl.Stop()

	err = bl.queue.stop()
	assert.EqualError(t, err, "Writes are blocked, possibly due to DropAll or Close")
}*/

func TestBlero_BadgerLogger(t *testing.T) {
	logger := &badgerLogger{}
	// test logger
	logger.Infof("[badgerLogger]TEST Infof")
	logger.Warningf("[badgerLogger]TEST Warningf")
	logger.Errorf("[badgerLogger]TEST Errorf")
}

func TestBlero_EnqueueJob(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	q := bl.queue

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	jName := "TestJob"
	jData := []byte("TestJob Args")

	jID, err := bl.EnqueueJob(jName, jData)
	assert.NoError(t, err)

	assert.Equal(t, uint64(1), jID)

	var j *Job
	err = q.db.View(func(txn *badger.Txn) error {
		j, err = getJobForKey(txn, []byte("q:pending:"+jIDString(jID)))
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	assert.Equal(t, jID, j.ID)
	assert.Equal(t, jName, j.Name)
	assert.Equal(t, jData, j.Data)
}

func TestBlero_EnqueueJob_Concurrent(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	ch := make(chan uint64)
	go func() {
		id, err := bl.EnqueueJob("TestJob", nil)
		assert.NoError(t, err)

		ch <- id
	}()

	go func() {
		id, err := bl.EnqueueJob("TestJob", nil)
		assert.NoError(t, err)

		ch <- id
	}()

	id1 := <-ch
	id2 := <-ch

	assert.ElementsMatch(t, []uint64{1, 2}, []uint64{id1, id2})
}

func TestBlero_EnqueueJobQueueStopped(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// delete folder
	defer deleteDBFolder(testDBPath)
	bl.Stop()

	_, err = bl.EnqueueJob("TestJob", nil)
	assert.EqualError(t, err, "DB Closed")
}

func TestBlero_DequeueJob(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	q := bl.queue

	j1Name := "TestJob"
	j1ID, err := bl.EnqueueJob(j1Name, nil)
	assert.NoError(t, err)

	j2Name := "TestJob"
	j2ID, err := bl.EnqueueJob(j2Name, nil)
	assert.NoError(t, err)

	j, err := q.dequeueJob()
	assert.NoError(t, err)

	assert.Equal(t, j1ID, j.ID)
	assert.Equal(t, j1Name, j.Name)

	err = q.db.View(func(txn *badger.Txn) error {
		// check that job 1 is not in the pending queue anymore
		_, err := txn.Get([]byte("q:pending:" + jIDString(j1ID)))
		assert.EqualError(t, err, badger.ErrKeyNotFound.Error())

		// check that job 2 is still in the pending queue
		_, err = txn.Get([]byte("q:pending:" + jIDString(j2ID)))
		assert.NoError(t, err)

		// get job 1 from inprogress queue
		completeJob, err := getJobForKey(txn, []byte("q:inprogress:"+jIDString(j1ID)))
		assert.NoError(t, err)

		assert.Equal(t, j1ID, completeJob.ID)
		assert.Equal(t, j1Name, completeJob.Name)
		return nil
	})
	assert.NoError(t, err)
}

func TestBlero_DequeueJob_Concurrent(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	q := bl.queue

	j1Name := "TestJob"
	j1ID, err := bl.EnqueueJob(j1Name, nil)
	assert.NoError(t, err)

	j2Name := "TestJob"
	j2ID, err := bl.EnqueueJob(j2Name, nil)
	assert.NoError(t, err)

	ch := make(chan *Job)

	go func() {
		j, err := q.dequeueJob()
		assert.NoError(t, err)
		ch <- j
	}()

	go func() {
		j, err := q.dequeueJob()
		assert.NoError(t, err)
		ch <- j
	}()

	j1 := <-ch
	j2 := <-ch

	jobs := []*Job{j1, j2}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].ID < jobs[j].ID
	})

	assert.Equal(t, jobs[0].ID, j1ID)
	assert.Equal(t, jobs[0].Name, j1Name)

	assert.Equal(t, jobs[1].ID, j2ID)
	assert.Equal(t, jobs[1].Name, j2Name)
}

func TestBlero_MarkJobDone(t *testing.T) {
	bl := New(testDBPath)
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	q := bl.queue

	j1Name := "TestJob"
	j1ID, err := bl.EnqueueJob(j1Name, nil)
	assert.NoError(t, err)

	j2Name := "TestJob"
	j2ID, err := bl.EnqueueJob(j2Name, nil)
	assert.NoError(t, err)

	// move job 1 to inprogress
	_, err = q.dequeueJob()
	assert.NoError(t, err)
	// move job 2 to inprogress
	_, err = q.dequeueJob()
	assert.NoError(t, err)

	err = q.markJobDone(j1ID, jobComplete)
	assert.NoError(t, err)

	err = q.markJobDone(j2ID, jobFailed)
	assert.NoError(t, err)

	err = q.db.View(func(txn *badger.Txn) error {
		// check that job 1 is not in the inprogress queue anymore
		_, err := txn.Get([]byte("q:inprogress:" + jIDString(j1ID)))
		assert.EqualError(t, err, badger.ErrKeyNotFound.Error())

		// check that job 2 is not in the inprogress queue anymore
		_, err = txn.Get([]byte("q:inprogress:" + jIDString(j2ID)))
		assert.EqualError(t, err, badger.ErrKeyNotFound.Error())

		// check that job 1 is now in the complete queue
		completeJob, err := getJobForKey(txn, []byte("q:complete:"+jIDString(j1ID)))
		assert.NoError(t, err)

		assert.Equal(t, j1ID, completeJob.ID)
		assert.Equal(t, j1Name, completeJob.Name)

		failedJob, err := getJobForKey(txn, []byte("q:failed:"+jIDString(j2ID)))
		assert.NoError(t, err)

		assert.Equal(t, j2ID, failedJob.ID)
		assert.Equal(t, j2Name, failedJob.Name)

		return nil
	})
	assert.NoError(t, err)

	// check random job id is not in queue error
	err = q.markJobDone(uint64(4151231), jobComplete)
	assert.EqualError(t, err, "Key not found")

	// check moving job to pending error
	err = q.markJobDone(j2ID, jobPending)
	assert.EqualError(t, err, "Can only move to Complete or Failed Status")
}

func TestBlero_moveItemErr(t *testing.T) {
	txn := &badger.Txn{}
	err := moveItem(txn, nil, nil, nil)
	assert.EqualError(t, err, "No sets or deletes are allowed in a read-only transaction")
}

func TestBlero_decodeJobErr(t *testing.T) {
	_, err := decodeJob(nil)
	assert.EqualError(t, err, "EOF")
}
