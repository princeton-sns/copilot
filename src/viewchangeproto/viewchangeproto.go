package viewchangeproto

type ViewChangeRole uint8
const (
	ACTIVE ViewChangeRole = iota   // active in a formed view
	MANAGER                        // proposing a new view
	UNDERLING                      // invited to join a new view
)

type ViewChangeStatus uint8
const (
	PREPARING ViewChangeStatus = iota
	PREPARED
	ACCEPTING
	ACCEPTED
	INITIATING
	INITIATED
)

const (
	PILOT uint8 = iota
	COPILOT
)

type View struct {
	ViewId    int32
	PilotId   int32 // index of this pilot
	ReplicaId int32 // unique id of this pilot replica
	// TODO: add primary/backups list
}

type ViewChange struct {
	ViewManagerId int32
	PilotId       int32 // indicating electing pilot/copilot
	OldView       View
	NewViewId     int32
}

type ViewChangeOK struct {
	PilotId                 int32
	ViewId                  int32 // used to identify the associated ViewChange message
	LatestCommittedInstance int32
	LatestInstance          int32
	AcceptedView            View
}

type ViewChangeReject struct {
	PilotId   int32
	ViewId    int32 // used to identify the associated ViewChange message
	OldView   View
	NewViewId int32
}

type ViewChangeReply struct {
	PilotId                 int32
	ViewId                  int32 // used to identify the associated ViewChange message
	MaxSeenViewId           int32
	OK                      uint8
	/* Next 3 fields for OK = true */
	LatestCommittedInstance int32
	LatestInstance          int32
	AcceptedView            View

	OldView                 View /*used by OK = FALSE*/
}

type AcceptView struct {
	ViewManagerId           int32
	PilotId                 int32
	LatestCommittedInstance int32
	LatestInstance          int32
	NewView                 View
}

type AcceptViewReply struct {
	PilotId int32
	ViewId  int32
	OK      uint8
}

type InitView struct {
	// TODO: consider adding LatestInstance
	PilotId        int32
	NewView        View
	LatestInstance int32
}