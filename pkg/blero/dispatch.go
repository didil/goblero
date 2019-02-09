package blero

import "fmt"

// Job represents a Goblero job definition
type Job struct {
	ID   uint64
	Name string
}

// Processor interface
type Processor interface {
	Run(j *Job) error
}

// ProcessorFunc is a processor function
type ProcessorFunc func(j *Job) error

// Run allows using ProcessorFunc as a Processor
func (pf ProcessorFunc) Run(j *Job) error {
	return pf(j)
}

// RegisterProcessor registers a new processor
func (bl *Blero) RegisterProcessor(p Processor) int {
	bl.dispatchL.Lock()
	defer bl.dispatchL.Unlock()

	bl.maxProcessorID++
	bl.processors[bl.maxProcessorID] = p
	return bl.maxProcessorID
}

// UnregisterProcessor unregisters a processor
// No more jobs will be assigned but if will not cancel a job that already started processing
func (bl *Blero) UnregisterProcessor(pID int) {
	bl.dispatchL.Lock()
	defer bl.dispatchL.Unlock()

	delete(bl.processors, pID)
}

// assignJobs assigns pending jobs to free processors
func (bl *Blero) assignJobs() error {
	bl.dispatchL.Lock()
	defer bl.dispatchL.Unlock()

	for pID := range bl.processors {
		if _, ok := bl.processing[pID]; !ok {
			err := bl.assignJob(pID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// assignJob assigns a pending job processor #pID and starts the run
// NOT THREAD SAFE !! only call from assignJobs
func (bl *Blero) assignJob(pID int) error {
	p := bl.processors[pID]
	if p == nil {
		return fmt.Errorf("Processor %v not found", pID)
	}

	j, err := bl.dequeueJob()
	if err != nil {
		return err
	}
	// no jobs to assign
	if j == nil {
		return nil
	}

	fmt.Printf("Assigning job %v to processor %v\n", j.ID, pID)

	if _, ok := bl.processing[pID]; ok {
		return fmt.Errorf("Cannot assign job %v to Processor %v. Processor busy with %v", j.ID, pID, bl.processing[pID])
	}

	bl.processing[pID] = j.ID
	go bl.runJob(pID, p, j)

	return nil
}

// unassignJob unmarks a job as assigned to #pID
func (bl *Blero) unassignJob(pID int) {
	bl.dispatchL.Lock()
	defer bl.dispatchL.Unlock()

	delete(bl.processing, pID)
}

// runJob runs a job on the corresponding processor and moves it to the right queue depending on results
func (bl *Blero) runJob(pID int, p Processor, j *Job) {
	defer bl.unassignJob(pID)
	err := p.Run(j)
	if err != nil {
		fmt.Printf("Processor: %v. Job %v failed with err: %v\n", pID, j.ID, err)
		err := bl.markJobDone(j.ID, JobFailed)
		if err != nil {
			fmt.Printf("markJobDone -> %v JobFailed failed: %v\n", j.ID, err)
		}
		return
	}

	err = bl.markJobDone(j.ID, JobComplete)
	if err != nil {
		fmt.Printf("markJobDone -> %v JobComplete failed: %v\n", j.ID, err)
	}
}
