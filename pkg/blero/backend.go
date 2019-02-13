package blero

import "fmt"

// Blero struct
type Blero struct {
	dispatcher *dispatcher
	queue      *queue
}

// New creates new Blero Backend
func New(dbPath string) *Blero {
	bl := &Blero{}
	pStore := newProcessorsStore()
	bl.dispatcher = newDispatcher(pStore)
	bl.queue = newQueue(queueOpts{DBPath: dbPath})
	return bl
}

// Start Blero
func (bl *Blero) Start() error {
	fmt.Println("Starting Blero ...")
	err := bl.queue.start()
	if err != nil {
		return err
	}
	bl.dispatcher.startLoop(bl.queue)
	return nil
}

// Stop Blero and Release resources
func (bl *Blero) Stop() error {
	fmt.Println("Stopping Blero ...")
	bl.dispatcher.stopLoop()
	return bl.queue.stop()
}

// EnqueueJob enqueues a new Job and returns the job id
func (bl *Blero) EnqueueJob(name string, data []byte) (uint64, error) {
	jID, err := bl.queue.enqueueJob(name, data)
	if err != nil {
		return 0, err
	}

	// signal that a new job was enqueued
	bl.dispatcher.signalLoop()

	return jID, nil
}

// EnqueueJobs enqueues new Jobs
/*func (bl *Blero) EnqueueJobs(names string) (uint64, error) {

}*/

// RegisterProcessor registers a new processor and returns the processor id
func (bl *Blero) RegisterProcessor(p Processor) int {
	return bl.dispatcher.registerProcessor(p)
}

// RegisterProcessorFunc registers a new ProcessorFunc and returns the processor id
func (bl *Blero) RegisterProcessorFunc(f func(j *Job) error) int {
	return bl.dispatcher.registerProcessor(ProcessorFunc(f))
}

// UnregisterProcessor unregisters a processor
// No more jobs will be assigned but if will not cancel a job that already started processing
func (bl *Blero) UnregisterProcessor(pID int) {
	bl.dispatcher.unregisterProcessor(pID)
}
