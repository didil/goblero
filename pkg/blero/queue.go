package blero

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger"
)

// QueueOpts struct
type QueueOpts struct {
	DBPath string
	Logger badger.Logger
}

// Queue struct
type Queue struct {
	opts QueueOpts
	db   *badger.DB
	seq  *badger.Sequence
	dbL  sync.Mutex
}

// NewQueue creates new Queue
func NewQueue(opts QueueOpts) *Queue {
	q := &Queue{opts: opts}
	return q
}

// Start Queue
func (q *Queue) Start() error {
	// validate opts
	if q.opts.DBPath == "" {
		return errors.New("Opts: DBPath is required")
	}

	// open db
	badgerOpts := badger.DefaultOptions
	badgerOpts.Dir = q.opts.DBPath
	badgerOpts.ValueDir = q.opts.DBPath
	if q.opts.Logger != nil {
		badgerOpts.Logger = q.opts.Logger
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return err
	}
	q.db = db

	// init sequence
	seq, err := db.GetSequence([]byte("standard"), 1000)
	if err != nil {
		return err
	}
	q.seq = seq

	return nil
}

// Stop Queue and Release resources
func (q *Queue) Stop() error {
	// release sequence
	err := q.seq.Release()
	if err != nil {
		return err
	}

	// close db
	err = q.db.Close()
	if err != nil {
		return err
	}

	return nil
}

// EnqueueJob enqueues a new Job to the Pending queue
func (q *Queue) EnqueueJob(name string) (uint64, error) {
	num, err := q.seq.Next()
	if err != nil {
		return 0, err
	}
	j := &Job{ID: num + 1, Name: name}

	err = q.db.Update(func(txn *badger.Txn) error {
		var b bytes.Buffer
		err := gob.NewEncoder(&b).Encode(j)
		if err != nil {
			return err
		}

		key := getJobKey(JobPending, j.ID)
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

	return j.ID, nil
}

// JobStatus Enum Type
type JobStatus uint8

const (
	// JobPending : waiting to be processed
	JobPending JobStatus = iota
	// JobInProgress : processing in progress
	JobInProgress
	// JobComplete : processing complete
	JobComplete
	// JobFailed : processing errored out
	JobFailed
)

func getQueueKeyPrefix(status JobStatus) string {
	return fmt.Sprintf("q:%v:", status)
}

func getJobKey(status JobStatus, ID uint64) string {
	return getQueueKeyPrefix(status) + strconv.Itoa(int(ID))
}

// dequeueJob moves the next pending job from the pending status to inprogress
func (q *Queue) dequeueJob() (*Job, error) {
	var j *Job

	q.dbL.Lock()
	defer q.dbL.Unlock()
	err := q.db.Update(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		prefix := []byte(getQueueKeyPrefix(JobPending))

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
		keyInProgress := getJobKey(JobInProgress, j.ID)
		err = txn.Set([]byte(keyInProgress), b)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return j, nil
}

// markJobDone moves a job from the inprogress status to complete/failed
func (q *Queue) markJobDone(id uint64, status JobStatus) error {
	if status != JobComplete && status != JobFailed {
		return errors.New("Can only move to Complete or Failed Status")
	}

	q.dbL.Lock()
	defer q.dbL.Unlock()
	err := q.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(getJobKey(JobInProgress, id)))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("Job %v not found in InProgress queue", id)
		}
		if err != nil {
			return err
		}

		b, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// remove from InProgress queue
		err = txn.Delete(item.Key())
		if err != nil {
			return err
		}

		// create in dest queue
		destKey := getJobKey(status, id)
		err = txn.Set([]byte(destKey), b)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
