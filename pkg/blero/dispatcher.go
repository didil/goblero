package blero

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"

	"github.com/dgraph-io/badger"
)

// EnqueueJob enqueues a new Job to the Pending queue
func (bl *Blero) EnqueueJob(name string) (uint64, error) {
	num, err := bl.seq.Next()
	if err != nil {
		log.Fatal(err)
	}
	j := &Job{ID: num + 1, Name: name}

	err = bl.db.Update(func(txn *badger.Txn) error {
		var b bytes.Buffer
		err := gob.NewEncoder(&b).Encode(j)
		if err != nil {
			return err
		}

		key := getJobKey(Pending, j.ID)
		fmt.Printf("Enqueing %v\n", key)
		err = txn.Set([]byte(key), b.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return j.ID, err
}

// JobStatus Enum Type
type JobStatus uint8

const (
	// Pending : waiting to be processed
	Pending JobStatus = iota
	// InProgress : processing in progress
	InProgress
	// Complete : processing complete
	Complete
	// Failed : processing errored out
	Failed
)

func getQueueKeyPrefix(status JobStatus) string {
	return fmt.Sprintf("q:%v:", status)
}

func getJobKey(status JobStatus, ID uint64) string {
	return getQueueKeyPrefix(status) + strconv.Itoa(int(ID))
}

// DequeueJob moves the next pending job from the pending status to inprogress and returns it
func (bl *Blero) DequeueJob() (*Job, error) {
	var j *Job

	bl.l.Lock()
	defer bl.l.Unlock()
	err := bl.db.Update(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		prefix := []byte(getQueueKeyPrefix(Pending))

		// go to smallest key after prefix
		it.Seek(prefix)
		defer it.Close()
		if !it.ValidForPrefix(prefix) {
			return nil // iteration is done, no job was found
		}

		item := it.Item()
		b, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&j)
		if err != nil {
			return err
		}

		// remove from Pending queue
		err = txn.Delete(item.Key())
		if err != nil {
			return err
		}

		// create in InProgress queue
		keyInProgress := (getQueueKeyPrefix(InProgress) + strconv.Itoa(int(j.ID)))
		err = txn.Set([]byte(keyInProgress), b)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return j, err
}
