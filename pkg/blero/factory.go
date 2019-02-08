package blero

import (
	"github.com/dgraph-io/badger"
)

// Opts struct
type Opts struct {
	DBPath string
}

// Blero struct
type Blero struct {
	opts Opts
	db   *badger.DB
	seq  *badger.Sequence
}

// New creates new Blero Backend
func New(opts Opts) *Blero {
	return &Blero{opts: opts}
}

// Start Blero
func (bl *Blero) Start() error {
	// open db
	badgerOpts := badger.DefaultOptions
	badgerOpts.Dir = bl.opts.DBPath
	badgerOpts.ValueDir = bl.opts.DBPath
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
