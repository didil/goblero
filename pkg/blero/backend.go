package blero

import (
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
	opts       Opts
	dispatcher *Dispatcher
	queue      *Queue
}

// New creates new Blero Backend
func New(opts Opts) *Blero {
	bl := &Blero{opts: opts}
	pStore := NewProcessorsStore()
	bl.dispatcher = NewDispatcher(pStore)
	bl.queue = NewQueue(QueueOpts{DBPath: opts.DBPath, Logger: opts.Logger})
	return bl
}

// Start Blero
func (bl *Blero) Start() error {
	err := bl.queue.Start()
	if err != nil {
		return err
	}
	bl.dispatcher.StartLoop(bl.queue)
	return nil
}

// Stop Blero and Release resources
func (bl *Blero) Stop() error {
	bl.dispatcher.StopLoop()
	return bl.queue.Stop()
}

// EnqueueJob enqueues a new Job
func (bl *Blero) EnqueueJob(name string) (uint64, error) {
	jID, err := bl.queue.EnqueueJob(name)
	if err != nil {
		return 0, err
	}

	go func() {
		// signal that a new job was enqueued
		bl.dispatcher.ch <- 1
	}()

	return jID, nil
}

// EnqueueJobs enqueues new Jobs
/*func (bl *Blero) EnqueueJobs(names string) (uint64, error) {

}*/

// RegisterProcessor registers a new processor
func (bl *Blero) RegisterProcessor(p Processor) int {
	return bl.dispatcher.RegisterProcessor(p)
}

// UnregisterProcessor unregisters a processor
// No more jobs will be assigned but if will not cancel a job that already started processing
func (bl *Blero) UnregisterProcessor(pID int) {
	bl.dispatcher.UnregisterProcessor(pID)
}
