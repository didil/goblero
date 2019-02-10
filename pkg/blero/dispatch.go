package blero

import (
	"fmt"
	"os"
	"sync"
)

// Dispatcher struct
type Dispatcher struct {
	dispatchL sync.Mutex
	ch        chan int
	quitCh    chan struct{}
	pStore    *ProcessorsStore
}

// NewDispatcher creates new Dispatcher
func NewDispatcher(pStore *ProcessorsStore) *Dispatcher {
	d := &Dispatcher{}
	d.ch = make(chan int, 100)
	d.quitCh = make(chan struct{})
	d.pStore = pStore
	return d
}

// StartLoop starts the dispatcher assignment loop
func (d *Dispatcher) StartLoop(q *Queue) {
	go func() {
		for {
			select {
			case <-d.ch:
				err := d.assignJobs(q)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Cannot assign jobs: %v", err)
				}
			case <-d.quitCh: // loop was stopped
				return
			}
		}
	}()
}

// StopLoop stops the dispatcher assignment loop
func (d *Dispatcher) StopLoop() {
	close(d.quitCh)
}

// RegisterProcessor registers a new processor
func (d *Dispatcher) RegisterProcessor(p Processor) int {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	pID := d.pStore.RegisterProcessor(p)

	go func() {
		// signal that the processor is now available
		d.ch <- 1
	}()

	return pID
}

// UnregisterProcessor unregisters a processor
// No more jobs will be assigned but if will not cancel a job that already started processing
func (d *Dispatcher) UnregisterProcessor(pID int) {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	d.pStore.UnregisterProcessor(pID)
}

// assignJobs assigns pending jobs from the queue to free processors
func (d *Dispatcher) assignJobs(q *Queue) error {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	pIDs := d.pStore.GetAvailableProcessorsIDs()

	for _, pID := range pIDs {
		err := d.assignJob(q, pID)
		if err != nil {
			return err
		}
	}

	return nil
}

// assignJob assigns a pending job processor #pID and starts the run
// NOT THREAD SAFE !! only call from assignJobs
func (d *Dispatcher) assignJob(q *Queue, pID int) error {
	p := d.pStore.GetProcessor(pID)
	if p == nil {
		return fmt.Errorf("Processor %v not found", pID)
	}

	j, err := q.dequeueJob()
	if err != nil {
		return err
	}
	// no jobs to assign
	if j == nil {
		return nil
	}

	fmt.Printf("Assigning job %v to processor %v\n", j.ID, pID)

	if d.pStore.IsProcessorBusy(pID) {
		return fmt.Errorf("Cannot assign job %v to Processor %v. Processor busy", j.ID, pID)
	}

	d.pStore.SetProcessing(pID, j.ID)

	go d.runJob(q, pID, p, j)

	return nil
}

// unassignJob unmarks a job as assigned to #pID
func (d *Dispatcher) unassignJob(pID int) {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	d.pStore.UnsetProcessing(pID)
}

// runJob runs a job on the corresponding processor and moves it to the right queue depending on results
func (d *Dispatcher) runJob(q *Queue, pID int, p Processor, j *Job) {
	defer d.processorDone(pID)
	err := p.Run(j)
	if err != nil {
		fmt.Printf("Processor: %v. Job %v failed with err: %v\n", pID, j.ID, err)
		err := q.markJobDone(j.ID, JobFailed)
		if err != nil {
			fmt.Printf("markJobDone -> %v JobFailed failed: %v\n", j.ID, err)
		}
		return
	}

	err = q.markJobDone(j.ID, JobComplete)
	if err != nil {
		fmt.Printf("markJobDone -> %v JobComplete failed: %v\n", j.ID, err)
	}
}

func (d *Dispatcher) processorDone(pID int) {
	d.unassignJob(pID)

	go func() {
		// signal that the processor might now be available
		d.ch <- 1
	}()
}
