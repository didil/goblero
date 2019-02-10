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