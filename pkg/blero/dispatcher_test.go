package blero

import (
	"bytes"
	"encoding/gob"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

const testDBPath = "../../db/test"

func deleteDBFolder(dbPath string) {
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

	j, err := bl.DequeueJob()
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

		var inProgressJob Job
		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&inProgressJob)
		assert.NoError(t, err)

		assert.Equal(t, j1ID, inProgressJob.ID)
		assert.Equal(t, j1Name, inProgressJob.Name)
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
		j, err := bl.DequeueJob()
		assert.NoError(t, err)
		ch <- j
	}()

	go func() {
		j, err := bl.DequeueJob()
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