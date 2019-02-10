package blero

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

// ProcessorsStore struct
type ProcessorsStore struct {
	maxProcessorID int
	processors     map[int]Processor
	processing     map[int]uint64
}

// NewProcessorsStore creates a new ProcessorsStore
func NewProcessorsStore() *ProcessorsStore {
	pStore := &ProcessorsStore{}
	pStore.processors = make(map[int]Processor)
	pStore.processing = make(map[int]uint64)
	return pStore
}

// RegisterProcessor registers a new processor
func (pStore *ProcessorsStore) RegisterProcessor(p Processor) int {
	pStore.maxProcessorID++
	pStore.processors[pStore.maxProcessorID] = p

	return pStore.maxProcessorID
}

// UnregisterProcessor unregisters a processor
func (pStore *ProcessorsStore) UnregisterProcessor(pID int) {
	delete(pStore.processors, pID)
}

// GetAvailableProcessorsIDs returns the currently free processors
func (pStore *ProcessorsStore) GetAvailableProcessorsIDs() []int {
	var pIDs []int
	for pID := range pStore.processors {
		if _, ok := pStore.processing[pID]; !ok {
			pIDs = append(pIDs, pID)
		}
	}
	return pIDs
}

// GetProcessor fetches processors by ID
func (pStore *ProcessorsStore) GetProcessor(pID int) Processor {
	return pStore.processors[pID]
}

// IsProcessorBusy checks if a processor is already working on a job
func (pStore *ProcessorsStore) IsProcessorBusy(pID int) bool {
	_, ok := pStore.processing[pID]
	return ok
}

// SetProcessing sets a processor as working on a job
func (pStore *ProcessorsStore) SetProcessing(pID int, jID uint64) {
	pStore.processing[pID] = jID
}

// UnsetProcessing unsets a processor as working on a job
func (pStore *ProcessorsStore) UnsetProcessing(pID int) {
	delete(pStore.processing, pID)
}
