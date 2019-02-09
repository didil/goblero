package blero

import (
	"errors"
	"sync"

	"github.com/dgraph-io/badger"
)

// Opts struct
type Opts struct {
	// Required
	// badger db folder path, the folder will be created if it doesn't exist
	DBPath string
	// Optional
	// badger.Logger interface logger
	Logger badger.Logger
}

// Blero struct
type Blero struct {
	opts           Opts
	db             *badger.DB
	seq            *badger.Sequence
	dbL            sync.Mutex
	dispatchL      sync.Mutex
	maxProcessorID int
	processors     map[int]Processor
	processing     map[int]uint64
}

// New creates new Blero Backend
func New(opts Opts) *Blero {
	bl := &Blero{opts: opts}
	bl.processors = make(map[int]Processor)
	bl.processing = make(map[int]uint64)
	return bl
}

// Start Blero
func (bl *Blero) Start() error {
	// validate opts
	if bl.opts.DBPath == "" {
		return errors.New("Opts: DBPath is required")
	}

	// open db
	badgerOpts := badger.DefaultOptions
	badgerOpts.Dir = bl.opts.DBPath
	badgerOpts.ValueDir = bl.opts.DBPath
	if bl.opts.Logger != nil {
		badgerOpts.Logger = bl.opts.Logger
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return err
	}
	bl.db = db

	// init sequence
	seq, err := db.GetSequence([]byte("standard"), 1000)
	if err != nil {
		return err
	}
	bl.seq = seq

	return nil
}

// Stop Blero and Release resources
func (bl *Blero) Stop() error {
	// release sequence
	err := bl.seq.Release()
	if err != nil {
		return err
	}

	// close db
	err = bl.db.Close()
	if err != nil {
		return err
	}

	return nil
}
