package masterproto

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	LeaderId int
}

type GetReplicaListArgs struct {
}

type GetReplicaListReply struct {
	ReplicaList []string
	Ready       bool
}

// Slowdown: Get two leaders reply
type GetTwoLeadersArgs struct {
}

type GetTwoLeadersReply struct {
	Leader1Id int
	Leader2Id int

}