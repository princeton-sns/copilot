package copilotproto

import (
	"state"
)

type Prepare struct {
	LeaderId int32
	Replica  int32
	Instance int32
	ViewId   int32
	Ballot   int32
}

type PrepareReply struct {
	AcceptorId  int32
	Replica     int32
	Instance    int32
	OK          uint8
	ViewId      int32
	Ballot      int32
	Status      int8
	Command     []state.Command
	Deps        []int32
	NullDepSafe uint8
	DepViewId   int32
}

type PreAccept struct {
	LeaderId  int32
	Replica   int32
	Instance  int32
	ViewId    int32
	Ballot    int32
	Command   []state.Command
	Deps      []int32
	DepViewId int32 /*View Id of dependency. Used for null dep checking*/
}

type PreAcceptReply struct {
	Replica       int32
	Instance      int32
	OK            uint8
	ViewId        int32
	Ballot        int32
	Deps          []int32
	CommittedDeps []int32
	DepSeen       uint8
}

type PreAcceptOK struct {
	Instance int32
}

type Accept struct {
	LeaderId  int32
	Replica   int32
	Instance  int32
	Ballot    int32
	ViewId    int32
	Command   []state.Command
	Deps      []int32
	DepViewId int32 /*View Id of dependency. Used for null dep checking*/
}

type AcceptReply struct {
	Replica  int32
	Instance int32
	OK       uint8
	ViewId   int32
	Ballot   int32
	DepSeen  uint8
}

type Commit struct {
	LeaderId    int32
	Replica     int32
	Instance    int32
	Command     []state.Command
	Deps        []int32
	NullDepSafe uint8
	DepViewId   int32
}

type CommitShort struct {
	LeaderId    int32
	Replica     int32
	Instance    int32
	Count       int32
	Deps        []int32
	NullDepSafe uint8
	DepViewId   int32
}

type TryPreAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Deps     []int32
}

type TryPreAcceptReply struct {
	AcceptorId       int32
	Replica          int32
	Instance         int32
	OK               uint8
	Ballot           int32
	ConflictReplica  int32
	ConflictInstance int32
	ConflictStatus   int8
}

const (
	NONE int8 = iota
	PREACCEPTED
	PREACCEPTED_EQ
	ACCEPTED
	COMMITTED
	EXECUTED
)
