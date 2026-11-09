package app

// StepError is a port failure that is not a business refusal: Step names what
// was being done ("load promotion", "count redemptions", ...). A transport
// logs it and answers 500.
type StepError struct {
	Step string
	Err  error
}

func (e *StepError) Error() string { return e.Step + ": " + e.Err.Error() }
func (e *StepError) Unwrap() error { return e.Err }

func step(name string, err error) error { return &StepError{Step: name, Err: err} }
