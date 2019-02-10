package blero

import (
	"fmt"
	"sync"
)

// Dispatcher struct
type Dispatcher struct {
	dispatchL      sync.Mutex
	maxProcessorID int
	processors     map[int]Processor
	processing     map[int]uint64
}

// NewDispatcher creates new Dispatcher
func NewDispatcher() *Dispatcher {
	d := &Dispatcher{}
	d.processors = make(map[int]Processor)
	d.processing = make(map[int]uint64)
	return d
}

// RegisterProcessor registers a new processor
func (d *Dispatcher) RegisterProcessor(p Processor) int {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	d.maxProcessorID++
	d.processors[d.maxProcessorID] = p
	return d.maxProcessorID
}

// UnregisterProcessor unregisters a processor
// No more jobs will be assigned but if will not cancel a job that already started processing
func (d *Dispatcher) UnregisterProcessor(pID int) {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	delete(d.processors, pID)
}

// assignJobs assigns pending jobs from the queue to free processors
func (d *Dispatcher) assignJobs(q *Queue) error {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	for pID := range d.processors {
		if _, ok := d.processing[pID]; !ok {
			err := d.assignJob(q, pID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// assignJob assigns a pending job processor #pID and starts the run
// NOT THREAD SAFE !! only call from assignJobs
func (d *Dispatcher) assignJob(q *Queue, pID int) error {
	p := d.processors[pID]
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

	if _, ok := d.processing[pID]; ok {
		return fmt.Errorf("Cannot assign job %v to Processor %v. Processor busy with %v", j.ID, pID, d.processing[pID])
	}

	d.processing[pID] = j.ID
	go d.runJob(q, pID, p, j)

	return nil
}

// unassignJob unmarks a job as assigned to #pID
func (d *Dispatcher) unassignJob(pID int) {
	d.dispatchL.Lock()
	defer d.dispatchL.Unlock()

	delete(d.processing, pID)
}

// runJob runs a job on the corresponding processor and moves it to the right queue depending on results
func (d *Dispatcher) runJob(q *Queue, pID int, p Processor, j *Job) {
	defer d.unassignJob(pID)
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
