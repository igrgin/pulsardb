package command

type TaskStatus int

const (
	Pending TaskStatus = iota
	Active
	Completed
	Failed
)

func (s TaskStatus) String() string {
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
