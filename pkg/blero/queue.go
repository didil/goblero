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
		b, err := encodeJob(j)
		if err != nil {
			return err
		}

		key := getJobKey(JobPending, j.ID)
		fmt.Printf("Enqueing %v\n", key)
		err = txn.Set([]byte(key), b)

		return err
	})
	if err != nil {
		return 0, err
	}

	return j.ID, nil
}

func encodeJob(j *Job) ([]byte, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(j)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
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
		prefix := []byte(getQueueKeyPrefix(JobPending))
		k, v, err := getFirstKVForPrefix(txn, prefix)
		if err != nil {
			return err
		}
		// iteration is done, no job was found
		if k == nil {
			return nil
		}

		j, err = decodeJob(v)
		if err != nil {
			return err
		}

		// Move from from Pending queue to InProgress queue
		err = moveItem(txn, k, []byte(getJobKey(JobInProgress, j.ID)), v)

		return err
	})

	return j, err
}

func getFirstKVForPrefix(txn *badger.Txn, prefix []byte) ([]byte, []byte, error) {
	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchValues = false
	it := txn.NewIterator(badger.DefaultIteratorOptions)

	// go to smallest key after prefix
	it.Seek(prefix)
	defer it.Close()
	// iteration done, no item found
	if !it.ValidForPrefix(prefix) {
		return nil, nil, nil
	}

	item := it.Item()

	k := item.KeyCopy(nil)

	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}

	return k, v, nil
}

// markJobDone moves a job from the inprogress status to complete/failed
func (q *Queue) markJobDone(id uint64, status JobStatus) error {
	if status != JobComplete && status != JobFailed {
		return errors.New("Can only move to Complete or Failed Status")
	}

	q.dbL.Lock()
	defer q.dbL.Unlock()
	err := q.db.Update(func(txn *badger.Txn) error {
		key := []byte(getJobKey(JobInProgress, id))
		b, err := getBytesForKey(txn, key)
		if err != nil {
			return err
		}

		// Move from from InProgress queue to dest queue
		err = moveItem(txn, key, []byte(getJobKey(status, id)), b)

		return err
	})

	return err
}

func decodeJob(b []byte) (*Job, error) {
	var j *Job
	err := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&j)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func getJobForKey(txn *badger.Txn, key []byte) (*Job, error) {
	b, err := getBytesForKey(txn, key)
	if err != nil {
		return nil, err
	}
	j, err := decodeJob(b)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func getBytesForKey(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	b, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func moveItem(txn *badger.Txn, oldKey []byte, newKey []byte, b []byte) error {
	// remove from Source queue
	err := txn.Delete(oldKey)
	if err != nil {
		return err
	}

	// create in Dest queue
	err = txn.Set(newKey, b)

	return err
}
