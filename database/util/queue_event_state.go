package util

type DBEventStatus int

const (
	Pending DBEventStatus = iota
	Active
	Completed
	Failed
)

// String makes Status implement the fmt.Stringer interface for pretty printing.
func (s DBEventStatus) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Active:
		return "Active"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}
