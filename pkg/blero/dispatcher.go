package blero

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"

	"github.com/dgraph-io/badger"
)

// Enqueue a new Job
func (bl *Blero) EnqueueJob(name string) {
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

		key := getQueueKeyPrefix("standard", Pending) + strconv.Itoa(int(j.ID))
		fmt.Printf("Enqueing %v\n", key)
		err = txn.Set([]byte(key), b.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
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

func getQueueKeyPrefix(name string, status JobStatus) string {
	return fmt.Sprintf("q:%v:%v:", name, status)
}

// DequeueJob moves a job from the pending status to inprogress
func (bl *Blero) DequeueJob() (*Job, error) {
	var j *Job

	err := bl.db.Update(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		prefix := []byte(getQueueKeyPrefix("standard", Pending))

		// go to smallest key after prefix
		it.Seek(prefix)
		if !it.ValidForPrefix(prefix) {
			return nil // iteration is done, no job was found
		}

		item := it.Item()
		fmt.Println("IsDeletedOrExpired", item.IsDeletedOrExpired())
		defer it.Close()

		b, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&j)
		if err != nil {
			return err
		}

		fmt.Println("Deleting", string(item.Key()))
		// remove from Pending queue
		err = txn.Delete(item.Key())
		if err != nil {
			return err
		}

		// create in InProgress queue
		keyInProgress := (getQueueKeyPrefix("standard", Pending) + strconv.Itoa(int(j.ID)))
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
