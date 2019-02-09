package blero

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

const testDBPath = "../../db/test"

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

func TestBlero_EnqueueJob(t *testing.T) {
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	jName := "TestJob"
	jID, err := bl.EnqueueJob(jName)
	assert.NoError(t, err)

	assert.Equal(t, uint64(1), jID)

	var j Job
	err = bl.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("q:pending:" + strconv.Itoa(int(jID))))
		assert.NoError(t, err)

		b, err := item.ValueCopy(nil)
		assert.NoError(t, err)

		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&j)
		assert.NoError(t, err)

		return nil
	})
	assert.NoError(t, err)

	assert.Equal(t, jID, j.ID)
	assert.Equal(t, jName, j.Name)
}

func TestBlero_EnqueueJob_Concurrent(t *testing.T) {
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	ch := make(chan uint64)
	go func() {
		id, err := bl.EnqueueJob("TestJob")
		assert.NoError(t, err)

		ch <- id
	}()

	go func() {
		id, err := bl.EnqueueJob("TestJob")
		assert.NoError(t, err)

		ch <- id
	}()

	id1 := <-ch
	id2 := <-ch

	assert.ElementsMatch(t, []uint64{1, 2}, []uint64{id1, id2})
}

func TestBlero_DequeueJob(t *testing.T) {
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	j1Name := "TestJob"
	j1ID, err := bl.EnqueueJob(j1Name)
	assert.NoError(t, err)

	j2Name := "TestJob"
	j2ID, err := bl.EnqueueJob(j2Name)
	assert.NoError(t, err)

	j, err := bl.dequeueJob()
	assert.NoError(t, err)

	assert.Equal(t, j1ID, j.ID)
	assert.Equal(t, j1Name, j.Name)

	err = bl.db.View(func(txn *badger.Txn) error {
		// check that job 1 is not in the pending queue anymore
		_, err := txn.Get([]byte("q:pending:" + strconv.Itoa(int(j1ID))))
		assert.EqualError(t, err, badger.ErrKeyNotFound.Error())

		// check that job 2 is still in the pending queue
		_, err = txn.Get([]byte("q:pending:" + strconv.Itoa(int(j2ID))))
		assert.NoError(t, err)

		// check that job 1 is now in the inprogress queue
		item, err := txn.Get([]byte("q:inprogress:" + strconv.Itoa(int(j1ID))))
		assert.NoError(t, err)

		b, err := item.ValueCopy(nil)
		assert.NoError(t, err)

		var completeJob Job
		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&completeJob)
		assert.NoError(t, err)

		assert.Equal(t, j1ID, completeJob.ID)
		assert.Equal(t, j1Name, completeJob.Name)
		return nil
	})
	assert.NoError(t, err)
}

func TestBlero_DequeueJob_Concurrent(t *testing.T) {
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	j1Name := "TestJob"
	j1ID, err := bl.EnqueueJob(j1Name)
	assert.NoError(t, err)

	j2Name := "TestJob"
	j2ID, err := bl.EnqueueJob(j2Name)
	assert.NoError(t, err)

	ch := make(chan *Job)

	go func() {
		j, err := bl.dequeueJob()
		assert.NoError(t, err)
		ch <- j
	}()

	go func() {
		j, err := bl.dequeueJob()
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
	bl := New(Opts{DBPath: testDBPath})
	err := bl.Start()
	assert.NoError(t, err)

	// stop gracefully
	defer deleteDBFolder(testDBPath)
	defer bl.Stop()

	j1Name := "TestJob"
	j1ID, err := bl.EnqueueJob(j1Name)
	assert.NoError(t, err)

	j2Name := "TestJob"
	j2ID, err := bl.EnqueueJob(j2Name)
	assert.NoError(t, err)

	// move job 1 to inprogress
	_, err = bl.dequeueJob()
	assert.NoError(t, err)
	// move job 2 to inprogress
	_, err = bl.dequeueJob()
	assert.NoError(t, err)

	err = bl.markJobDone(j1ID, JobComplete)
	assert.NoError(t, err)

	err = bl.markJobDone(j2ID, JobFailed)
	assert.NoError(t, err)

	err = bl.db.View(func(txn *badger.Txn) error {
		// check that job 1 is not in the inprogress queue anymore
		_, err := txn.Get([]byte("q:inprogress:" + strconv.Itoa(int(j1ID))))
		assert.EqualError(t, err, badger.ErrKeyNotFound.Error())

		// check that job 2 is not in the inprogress queue anymore
		_, err = txn.Get([]byte("q:inprogress:" + strconv.Itoa(int(j2ID))))
		assert.EqualError(t, err, badger.ErrKeyNotFound.Error())

		// check that job 1 is now in the complete queue
		item1, err := txn.Get([]byte("q:complete:" + strconv.Itoa(int(j1ID))))
		assert.NoError(t, err)

		// check that job 2 is now in the failed queue
		item2, err := txn.Get([]byte("q:failed:" + strconv.Itoa(int(j2ID))))
		assert.NoError(t, err)

		b1, err := item1.ValueCopy(nil)
		assert.NoError(t, err)

		var completeJob Job
		err = gob.NewDecoder(bytes.NewBuffer(b1)).Decode(&completeJob)
		assert.NoError(t, err)

		assert.Equal(t, j1ID, completeJob.ID)
		assert.Equal(t, j1Name, completeJob.Name)

		b2, err := item2.ValueCopy(nil)
		assert.NoError(t, err)

		var failedJob Job
		err = gob.NewDecoder(bytes.NewBuffer(b2)).Decode(&failedJob)
		assert.NoError(t, err)

		assert.Equal(t, j2ID, failedJob.ID)
		assert.Equal(t, j2Name, failedJob.Name)

		return nil
	})
	assert.NoError(t, err)

	// check random job id is not in queue error
	err = bl.markJobDone(uint64(4151231), JobComplete)
	assert.EqualError(t, err, "Job 4151231 not found in InProgress queue")

	// check moving job to pending error
	err = bl.markJobDone(j2ID, JobPending)
	assert.EqualError(t, err, "Can only move to Complete or Failed Status")
}
