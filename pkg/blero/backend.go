package blero

import "fmt"

// Blero struct
type Blero struct {
	dispatcher *Dispatcher
	queue      *Queue
}

// New creates new Blero Backend
func New(dbPath string) *Blero {
	bl := &Blero{}
	pStore := NewProcessorsStore()
	bl.dispatcher = NewDispatcher(pStore)
	bl.queue = NewQueue(QueueOpts{DBPath: dbPath})
	return bl
}

// Start Blero
func (bl *Blero) Start() error {
	fmt.Println("Starting Blero ...")
	err := bl.queue.Start()
	if err != nil {
		return err
	}
	bl.dispatcher.StartLoop(bl.queue)
	return nil
}

// Stop Blero and Release resources
func (bl *Blero) Stop() error {
	fmt.Println("Stopping Blero ...")
	bl.dispatcher.StopLoop()
	return bl.queue.Stop()
}

// EnqueueJob enqueues a new Job
func (bl *Blero) EnqueueJob(name string, data []byte) (uint64, error) {
	jID, err := bl.queue.EnqueueJob(name, data)
	if err != nil {
		return 0, err
	}

	// signal that a new job was enqueued
	bl.dispatcher.SignalLoop()

	return jID, nil
}

// EnqueueJobs enqueues new Jobs
/*func (bl *Blero) EnqueueJobs(names string) (uint64, error) {

}*/

// RegisterProcessor registers a new processor
func (bl *Blero) RegisterProcessor(p Processor) int {
	return bl.dispatcher.RegisterProcessor(p)
}

// RegisterProcessorFunc registers a new processor
func (bl *Blero) RegisterProcessorFunc(f func(j *Job) error) int {
	return bl.dispatcher.RegisterProcessor(ProcessorFunc(f))
}

// UnregisterProcessor unregisters a processor
// No more jobs will be assigned but if will not cancel a job that already started processing
func (bl *Blero) UnregisterProcessor(pID int) {
	bl.dispatcher.UnregisterProcessor(pID)
}
