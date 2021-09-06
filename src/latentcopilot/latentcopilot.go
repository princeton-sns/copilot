package latentcopilot

import (
	"bloomfilter"
	"bufio"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"latentcopilotproto"
	"log"
	"math"
	"os"
	"sort"
	"state"
	"sync"
	"time"
	"viewchangeproto"
)

const NUM_LEADERS = 2
const DS = 1
const HIGH_PRIORITY_LEADER = 0
const MAX_CLIENTS = 100 // increase this if experimenting with >100 clients

const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10

// Heartbeat/Beacon
const BEACON_SENDING_INTERVAL = 100 * 1e6 // 100 ms
// Miss a beacon if time since last beacon received >= BEACON_MISS_INTERVAL
const BEACON_MISS_INTERVAL = 1000 * 1e6 // 110 ms
// Detect failure if miss >= BEACON_FAILURE_THRESHOLD beacons in a row
const BEACON_FAILURE_THRESHOLD = 1

// Ballot number is comprised of 3 parts:
// - subballot: 24 bits
// - replicaId of original proposer: 4 bits
// - replicaId of current owner: 4 bits
// total nbits should be 32 (we use int32 for ballot)
const REPLICA_ID_NBITS = 4
const REPLICA_ID_BITMASK = int32(1<<REPLICA_ID_NBITS - 1)

const MAX_BATCH = 5000
const BATCH_INTERVAL = 100 * time.Microsecond

const FAST_PATH_TIMEOUT = 5 * 1e6                 // 5ms
const COMMIT_GRACE_PERIOD = 10 * time.Millisecond // 10 ms

const BF_K = 4
const BF_M_N = 32.0

var bf_PT uint32

const DO_CHECKPOINTING = false
const HT_INIT_SIZE = 200000
const CHECKPOINT_PERIOD = 10000

// injecting slowdown
const INJECT_SLOWDOWN = false

// record execution stats
const RECORD_EXEC_STATS = false

// printing stats
const PRINT_STATS = false

const LATENCY_THRESHOLD = 180 * time.Second

// Enable/Disable Pingpong optimization 1
const PINGPONG = false
const PINGPONG_TIMEOUT_ENABLED = true
const PINGPONG_TIMEOUT = 1000 * time.Microsecond

// variable controlling toggle mode
const ADAPTIVE_TIMEOUT_ENABLED = false

// delay for latent copilot
const LATENT_PIILOT_DELAY = 10 * time.Millisecond
const SWITCHING_MODE_WINDOW = 100 * time.Millisecond
const MIN_NEW_PROPOSALS_TO_BECOME_ACTIVE = 50
const MIN_NEW_COMMITS_TO_BECOME_LATENT = 50

var adaptedPingPongTimeout = PINGPONG_TIMEOUT

var warmupDone = false

var cpMarker []state.Command
var cpcounter = 0

/* Helper map to check duplicating execution */
//var execMap map[state.OperationId]bool
type Result struct {
	//Cache   state.Value
	Replied bool
}

// Execution stats
type ExecStats struct {
	depWaits         uint64 // number of batches waiting for dep
	nullDepOKs       uint64 // number of batches successfully nullify deps
	numNullDepStrike uint64 // number of null deps that succeed in a row
	depExeced        uint64 // number of batches whose dep has been executed
	priority         uint64 // number of batches that can execute because of higher priority
}

type Stats struct {
	batches  uint64 // number of batches
	cmds     uint64 // number of commands
	fast     uint64 // number of instances succeeding on fast path
	slow     uint64 // number of instances running on slow path
	rpcsSent uint64 // number of messages sent
	rpcsRcvd uint64 // number of messages received

}

type ViewChangeState struct {
	// state of view
	role   viewchangeproto.ViewChangeRole
	status viewchangeproto.ViewChangeStatus

	// last (or current) formed view
	view *viewchangeproto.View

	// starting instance in the log of this view
	startInstance int32

	// default ballot for current view
	defaultBallot int32

	/// highest proposed new viewid
	proposedViewId int32

	// accepted new view (if any)
	acceptedView              *viewchangeproto.View
	acceptedCommittedInstance int32
	acceptedCurrentInstance   int32

	// View manager bookkeeping used by view manager
	vmb *ViewManagerBookkeeping
}

type ViewManagerBookkeeping struct {
	maxRecvViewId             int32
	maxCommittedInstance      int32
	maxCurrentInstance        int32
	acceptedView              *viewchangeproto.View
	acceptedCommittedInstance int32
	acceptedCurrentInstance   int32
	viewchangeOKs             int
	acceptViewOKs             int
	nacks                     int
}

type Replica struct {
	*genericsmr.Replica
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	commitShortChan       chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	viewchangeChan        chan fastrpc.Serializable
	viewchangeReplyChan   chan fastrpc.Serializable
	acceptViewChan        chan fastrpc.Serializable
	acceptViewReplyChan   chan fastrpc.Serializable
	initViewChan          chan fastrpc.Serializable
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	preAcceptOKRPC        uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	commitShortRPC        uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	viewchangeRPC         uint8
	viewchangeReplyRPC    uint8
	acceptViewRPC         uint8
	acceptViewReplyRPC    uint8
	initViewRPC           uint8
	InstanceSpace         [][]*Instance // the space of all instances (used and not yet used)
	crtInstance           []int32       // highest active instance numbers that this replica knows about
	CommittedUpTo         []int32       // highest committed instance per replica that this replica knows about
	ExecedUpTo            []int32       // instance up to which all commands have been executed (including iteslf)
	//exec                  *Exec
	conflicts          []map[state.Key]int32
	latestCPReplica    int32
	latestCPInstance   int32
	clientMutex        *sync.Mutex // for synchronizing when sending replies to clients from multiple go-routines
	instancesToRecover chan *instanceId
	IsLeader1          bool // does this replica think it is the leader 1
	IsLeader2          bool // does this replica think it is the leader 2
	slowPathChan       chan *instanceId
	replicaReply       bool      // does a non-leader replica reply to client
	preAcceptDeps      [][]int32 // preAcceptDeps[L][i] = the dependency this replica *pre-accepted* for L.i
	stat               Stats
	clientWriters      map[uint32]*bufio.Writer
	//execMap               map[int64]*Result
	//execMap             map[int64]bool
	execMap              []bool // keep track executed commands; could also use map; chose array for performance reason
	latestOps            []int32
	views                []*ViewChangeState
	beaconReceivedTimes  []time.Time
	beaconMisses         []int32
	onOffProposeChanReal chan *genericsmr.Propose
	latestTakeover       []int32 // latest instance that is fast taken over from each pilot
	committedMap         []bool
	IsActive             bool // active or latent pilot
	activeStartTime      time.Time
	numNewCommit         int32
	numNewProposals      int32
}

type Instance struct {
	Cmds           []state.Command
	ballot         int32
	Status         int8
	Deps           []int32
	lb             *LeaderBookkeeping
	Index, Lowlink int
	bfilter        *bloomfilter.Bloomfilter
	startTime      time.Time
	committedTime  time.Time
	nullDepSafe    bool
	depViewId      int32
	acceptBallot   int32
}

type instanceId struct {
	replica  int32
	instance int32
}

type RecoveryInstance struct {
	cmds                 []state.Command
	status               int8
	deps                 []int32
	preAcceptCount       int
	leaderResponded      bool
	prepareReplyDeps     []int32
	originalDepCount     int
	otherLeaderResponded bool
}

type LeaderBookkeeping struct {
	clientProposals    []*genericsmr.Propose
	maxRecvBallot      int32
	prepareOKs         int
	allEqual           bool
	preAcceptOKs       int
	preAcceptReplies   int
	preAcceptReplyDeps []int32
	acceptOKs          int
	nacks              int
	originalDeps       []int32
	committedDeps      []int32
	recoveryInst       *RecoveryInstance
	preparing          bool
	tryingToPreAccept  bool
	possibleQuorum     []bool
	tpaOKs             int
	numDepSeens        int
	depViewId          int32
	cmdCommitted       bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, beacon bool, durable bool, rreply bool) *Replica {
	r := &Replica{
		genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*2),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([][]*Instance, len(peerAddrList)), // TODO: this should be NUM_LEADERS instead of len(peerAddrList) to save space
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]map[state.Key]int32, len(peerAddrList)),
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, genericsmr.CHAN_BUFFER_SIZE),
		false,
		false,
		make(chan *instanceId, genericsmr.CHAN_BUFFER_SIZE),
		rreply,
		make([][]int32, NUM_LEADERS),
		Stats{batches: 0, cmds: 0, fast: 0, slow: 0, rpcsSent: 0, rpcsRcvd: 0},
		make(map[uint32]*bufio.Writer),
		//make(map[int64]bool, 1000),
		make([]bool, MAX_CLIENTS<<21),
		//make(map[int64]bool, 100000000),
		//make(map[int64]bool),
		//make(map[int64]*Result),
		make([]int32, MAX_CLIENTS),
		make([]*ViewChangeState, NUM_LEADERS), //TODO: init later in main
		make([]time.Time, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make(chan *genericsmr.Propose, 3*genericsmr.CHAN_BUFFER_SIZE),
		make([]int32, NUM_LEADERS),
		make([]bool, MAX_CLIENTS<<21),
		false,
		(time.Time{}),
		0,
		0,
	}

	// Uncomment this if r.latestOp is used
	/*
		for i := 0; i < len(r.latestOps); i++ {
			r.latestOps[i] = int32(-1)
		}
	*/

	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < NUM_LEADERS; i++ {
		r.InstanceSpace[i] = make([]*Instance, 2*1024*1024)
		r.crtInstance[i] = 0
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]int32, HT_INIT_SIZE)
		r.preAcceptDeps[i] = make([]int32, 2*1024*1024)

		// init view
		r.views[i] = &ViewChangeState{}
		r.views[i].role = viewchangeproto.ACTIVE
		// init replica i to be pilot i-th
		// TODO: maybe init viewid to be 0x|replicaId, which is i?
		r.views[i].proposedViewId = 0
		r.views[i].view = &viewchangeproto.View{0, int32(i), int32(i)}
		r.views[i].startInstance = 0
		r.views[i].defaultBallot = makeBallot(0, int32(i), int32(i))

		// latest fast takeover for latent copilot
		r.latestTakeover[i] = -1
	}

	for bf_PT = 1; math.Pow(2, float64(bf_PT))/float64(MAX_BATCH) < BF_M_N; {
		bf_PT++
	}

	cpMarker = make([]state.Command, 0)

	//register RPCs
	r.prepareRPC = r.RegisterRPC(new(latentcopilotproto.Prepare), r.prepareChan)
	r.prepareReplyRPC = r.RegisterRPC(new(latentcopilotproto.PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = r.RegisterRPC(new(latentcopilotproto.PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RegisterRPC(new(latentcopilotproto.PreAcceptReply), r.preAcceptReplyChan)
	r.preAcceptOKRPC = r.RegisterRPC(new(latentcopilotproto.PreAcceptOK), r.preAcceptOKChan)
	r.acceptRPC = r.RegisterRPC(new(latentcopilotproto.Accept), r.acceptChan)
	r.acceptReplyRPC = r.RegisterRPC(new(latentcopilotproto.AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RegisterRPC(new(latentcopilotproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(latentcopilotproto.CommitShort), r.commitShortChan)
	//r.tryPreAcceptRPC = r.RegisterRPC(new(latentcopilotproto.TryPreAccept), r.tryPreAcceptChan)
	//r.tryPreAcceptReplyRPC = r.RegisterRPC(new(latentcopilotproto.TryPreAcceptReply), r.tryPreAcceptReplyChan)
	r.viewchangeRPC = r.RegisterRPC(new(viewchangeproto.ViewChange), r.viewchangeChan)
	r.viewchangeReplyRPC = r.RegisterRPC(new(viewchangeproto.ViewChangeReply), r.viewchangeReplyChan)
	r.acceptViewRPC = r.RegisterRPC(new(viewchangeproto.AcceptView), r.acceptViewChan)
	r.acceptViewReplyRPC = r.RegisterRPC(new(viewchangeproto.AcceptViewReply), r.acceptViewReplyChan)
	r.initViewRPC = r.RegisterRPC(new(viewchangeproto.InitView), r.initViewChan)

	go r.run()

	return r
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	b := make([]byte, 5+r.N*4)
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.Status)
	l := 5
	for _, dep := range inst.Deps {
		binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
		l += 4
	}
	r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

func (r *Replica) pingpongTimeoutClock() {
	if r.IsLeader2 {
		time.Sleep(2 * PINGPONG_TIMEOUT)
	}
	for !r.Shutdown && (r.IsLeader1 || r.IsLeader2) {
		time.Sleep(PINGPONG_TIMEOUT)
		select {
		case fastClockChan <- true:
		default:
			break
		}
	}
}

/* Clock goroutine */
var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(BATCH_INTERVAL) // 0.1 ms
		fastClockChan <- true
	}
}

func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(BEACON_SENDING_INTERVAL) // 150 ms
		slowClockChan <- true
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	log.Println(r.PreferredPeerOrder)
}

// Manage Client Writers
func (r *Replica) registerClient(clientId uint32, writer *bufio.Writer) uint8 {
	w, exists := r.clientWriters[clientId]

	if !exists {
		r.clientWriters[clientId] = writer
		return TRUE
	}

	if w == writer {
		return TRUE
	}

	return FALSE
}

var conflicted, weird, slow, happy int

/* ============= */

/***********************************
   Main event processing loop      *
************************************/

func (r *Replica) run() {
	r.ConnectToPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	if r.Thrifty {
		for i := 0; i < r.N; i++ {
			r.PreferredPeerOrder[i] = int32(i)
		}
	} else if r.Id == 0 {
		//init quorum read lease
		quorum := make([]int32, r.N/2+1)
		for i := 0; i <= r.N/2; i++ {
			quorum[i] = int32(i)
		}
		r.UpdatePreferredPeerOrder(quorum)
	}

	// initialize: replica id 0 as pilot 0, replica id 1 as pilot 1
	// pilot 0 as active pilot and pilot 1 as latent pilot
	if r.Id == int32(0) {
		r.IsLeader1 = true
		r.IsActive = true
		r.activeStartTime = time.Now()
		dlog.Println("LEADER 1 PID:", os.Getpid(), os.Getppid())
	}
	if r.Id == int32(1) {
		r.IsLeader2 = true
		dlog.Println("LEADER 2 PID:", os.Getpid(), os.Getppid())
	}

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)

	if PINGPONG && PINGPONG_TIMEOUT_ENABLED && (r.IsLeader1 || r.IsLeader2) {
		go r.pingpongTimeoutClock()
	}

	go r.slowClock()

	if MAX_BATCH > 100 && !PINGPONG {
		go r.fastClock()
	}

	if r.Beacon {
		go r.stopAdapting()
	}

	onOffProposeChan := r.ProposeChan

	// give the ball to leader 1
	if PINGPONG && r.IsLeader2 {
		onOffProposeChan = nil
	}

	go r.executeCommands()

	// GC debug
	//var garPercent = flag.Int("garC", 50, "Collect info about GC")
	//debug.SetGCPercent(*garPercent)
	//gcTicker := time.NewTicker(50 * time.Second)

	var timer05ms *time.Timer
	var timer1ms *time.Timer
	var timer2ms *time.Timer
	var timer5ms *time.Timer
	var timer10ms *time.Timer
	var timer20ms *time.Timer
	var timer40ms *time.Timer
	var timer80ms *time.Timer
	allFired := false
	if INJECT_SLOWDOWN {
		timer05ms = time.NewTimer(48 * time.Second)
		timer1ms = time.NewTimer(49 * time.Second)
		timer2ms = time.NewTimer(50 * time.Second)
		timer5ms = time.NewTimer(51 * time.Second)
		timer10ms = time.NewTimer(52 * time.Second)
		timer20ms = time.NewTimer(53 * time.Second)
		timer40ms = time.NewTimer(54 * time.Second)
		timer80ms = time.NewTimer(55 * time.Second)
	}
	// lastSent := time.Now()

	/* Testing viewchange */
	/*TEST_VC := false
	var vcTimer *time.Timer
	if r.Id == int32(2) {
		vcTimer = time.NewTimer(15 * time.Second)
	}*/

	// warmupDone := false
	warmupTimer := time.NewTimer(30 * time.Second)

	for !r.Shutdown {

		/*if TEST_VC && r.Id == int32(2) {
			select {
			case <-vcTimer.C:
				fmt.Printf("Replica %v is starting view change for pilot 0 at %v\n", r.Id, time.Now())
				r.startViewChange(0)
				break
			default:
				break
			}
		}*/
		if !warmupDone {
			select {
			case <-warmupTimer.C:
				fmt.Printf("Warmup period is over. Start detecting failure based on heartbeats...\n")
				warmupDone = true

			default:
				break
			}
		}

		// if r.IsActive && INJECT_SLOWDOWN && !allFired {
		if r.IsLeader1 && INJECT_SLOWDOWN && !allFired {
			select {
			case <-timer05ms.C:
				fmt.Printf("Replica %v: Timer 0.5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(500 * time.Microsecond)

			case <-timer1ms.C:
				fmt.Printf("Replica %v: Timer 1ms fired at %v\n", r.Id, time.Now())
				time.Sleep(1 * time.Millisecond)

			case <-timer2ms.C:
				fmt.Printf("Replica %v: Timer 2ms fired at %v\n", r.Id, time.Now())
				time.Sleep(2 * time.Millisecond)

			case <-timer5ms.C:
				fmt.Printf("Replica %v: Timer 5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(5 * time.Millisecond)

			case <-timer10ms.C:
				fmt.Printf("Replica %v: Timer 10ms fired at %v\n", r.Id, time.Now())
				time.Sleep(10 * time.Millisecond)

			case <-timer20ms.C:
				fmt.Printf("Replica %v: Timer 20ms fired at %v\n", r.Id, time.Now())
				time.Sleep(20 * time.Millisecond)

			case <-timer40ms.C:
				fmt.Printf("Replica %v: Timer 40ms fired at %v\n", r.Id, time.Now())
				time.Sleep(40 * time.Millisecond)

			case <-timer80ms.C:
				fmt.Printf("Replica %v: Timer 80ms fired at %v\n", r.Id, time.Now())
				allFired = true
				time.Sleep(80 * time.Millisecond)

			default:
				break

			}
		}

		select {
		case propose := <-onOffProposeChan:
			if !(r.IsLeader1 || r.IsLeader2) {
				break
			}
			if r.IsActive {
				// active copilot goes through normal round
				r.handlePropose(propose)
				if MAX_BATCH > 100 || PINGPONG {
					onOffProposeChan = nil
				}
				// lastSent = time.Now()
			} else {
				batchSize := len(r.ProposeChan) + 1
				if batchSize > MAX_BATCH {
					batchSize = MAX_BATCH
				}
				proposals := make([]*genericsmr.Propose, batchSize)
				proposals[0] = propose
				for i := 1; i < batchSize; i++ {
					proposals[i] = <-r.ProposeChan
				}
				go func() {
					time.Sleep(LATENT_PIILOT_DELAY)
					for i := 0; i < batchSize; i++ {
						r.onOffProposeChanReal <- proposals[i]
					}
				}()
				/*if MAX_BATCH > 100 || PINGPONG {
					onOffProposeChan = nil
				}
				lastSent = time.Now()*/
			}

		case propose := <-r.onOffProposeChanReal:
			//got a Propose from a client
			dlog.Printf("Proposal with op %d\n", propose.Command.Op)
			//st := time.Now()
			//r.handlePropose(propose)
			r.handleProposeForLatentPilot(propose)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-propose takes %v\n", r.Id, time.Since(st))
			}*/
			//deactivate new proposals channel to prioritize the handling of other protocol messages,
			//and to allow commands to accumulate for batching
			// pingpong: passed the ball, wait for the ball back
			if MAX_BATCH > 100 || PINGPONG {
				onOffProposeChan = nil
			}
			// lastSent = time.Now()

		case <-fastClockChan:
			onOffProposeChan = r.ProposeChan

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*latentcopilotproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare for instance %d.%d\n", prepare.Replica, prepare.Instance)
			// st := time.Now()
			r.handlePrepare(prepare)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-prepare takes %v\n", r.Id, time.Since(st))
			}*/

		case preAcceptS := <-r.preAcceptChan:
			preAccept := preAcceptS.(*latentcopilotproto.PreAccept)
			//got a PreAccept message
			dlog.Printf("Received PreAccept for instance %d.%d\n", preAccept.LeaderId, preAccept.Instance)
			// st := time.Now()
			r.handlePreAccept(preAccept)
			if !(r.IsLeader1 || r.IsLeader2) {
				break
			}
			// got the ball, it's my turn to propose batch
			if PINGPONG {
				onOffProposeChan = r.ProposeChan
			}

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*latentcopilotproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept for instance %d.%d\n", accept.LeaderId, accept.Instance)
			//st := time.Now()
			r.handleAccept(accept)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-acceptS takes %v\n", r.Id, time.Since(st))
			}*/

		case commitS := <-r.commitChan:
			commit := commitS.(*latentcopilotproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
			//st := time.Now()
			/*if warmupDone && r.IsActive && time.Since(r.activeStartTime) > SWITCHING_MODE_WINDOW && !r.checkCommandsCommitted(commit.Command) {
				r.IsActive = false
				r.activeStartTime = time.Now()
				fmt.Printf("Replica %v: becomes LATENT at %v\n", r.Id, time.Now())
			}*/
			if warmupDone && r.IsActive && time.Since(r.activeStartTime) > SWITCHING_MODE_WINDOW {
				committed := r.checkCommandsCommitted(commit.Command)
				if committed {
					r.numNewCommit = 0
				} else {
					r.numNewCommit++
				}
				if r.numNewCommit >= MIN_NEW_COMMITS_TO_BECOME_LATENT {
					r.IsActive = false
					r.activeStartTime = time.Now()
					r.numNewCommit = 0
					fmt.Printf("Replica %v: becomes LATENT at %v\n", r.Id, time.Now())
				}
			}
			r.handleCommit(commit)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-commitS takes %v\n", r.Id, time.Since(st))
			}*/

		case commitS := <-r.commitShortChan:
			commit := commitS.(*latentcopilotproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*latentcopilotproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d.%d\n", prepareReply.Replica, prepareReply.Instance)
			//st := time.Now()
			r.handlePrepareReply(prepareReply)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-prepareReplyS takes %v\n", r.Id, time.Since(st))
			}*/

		case preAcceptReplyS := <-r.preAcceptReplyChan:
			preAcceptReply := preAcceptReplyS.(*latentcopilotproto.PreAcceptReply)
			//got a PreAccept reply
			dlog.Printf("Received PreAcceptReply for instance %d.%d\n", preAcceptReply.Replica, preAcceptReply.Instance)
			//st := time.Now()
			r.handlePreAcceptReply(preAcceptReply)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-preAcceptReplyS takes %v\n", r.Id, time.Since(st))
			}*/

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*latentcopilotproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d.%d\n", acceptReply.Replica, acceptReply.Instance)
			//st := time.Now()
			r.handleAcceptReply(acceptReply)
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-acceptReplyS takes %v\n", r.Id, time.Since(st))
			}*/

		case viewchangeS := <-r.viewchangeChan:
			viewchange := viewchangeS.(*viewchangeproto.ViewChange)
			r.handleViewChange(viewchange)

		case acceptViewS := <-r.acceptViewChan:
			acceptView := acceptViewS.(*viewchangeproto.AcceptView)
			r.handleAcceptView(acceptView)

		case initViewS := <-r.initViewChan:
			initView := initViewS.(*viewchangeproto.InitView)
			r.handleStartView(initView)

		case viewchangeReplyS := <-r.viewchangeReplyChan:
			viewchangeReply := viewchangeReplyS.(*viewchangeproto.ViewChangeReply)
			r.handleViewChangeReply(viewchangeReply)

		case acceptViewReplyS := <-r.acceptViewReplyChan:
			acceptViewReply := acceptViewReplyS.(*viewchangeproto.AcceptViewReply)
			r.handleAcceptViewReply(acceptViewReply)

		case beacon := <-r.BeaconChan:
			dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			r.beaconReceivedTimes[beacon.Rid] = time.Now()
			// note: no need to reset miss here since it will be reset beolow
			r.beaconMisses[beacon.Rid] = 0
			r.Alive[beacon.Rid] = true

		case <-slowClockChan:
			// st := time.Now()
			// if r.Beacon {
			// Send beacons/heartbeats to other replicas
			for q := int32(0); q < int32(r.N); q++ {
				if q == r.Id {
					continue
				}
				r.SendBeacon(q)
			}
			// }
			// Check health status of each replica
			for q := int32(0); q < int32(r.N); q++ {
				if q == r.Id {
					continue
				}
				if warmupDone && (r.beaconReceivedTimes[q] == (time.Time{}) || time.Since(r.beaconReceivedTimes[q]) >= BEACON_MISS_INTERVAL) {
					//if r.beaconReceivedTimes[q] != (time.Time{}) && time.Since(r.beaconReceivedTimes[q]) >= BEACON_MISS_INTERVAL {
					r.beaconMisses[q]++
				} else {
					r.beaconMisses[q] = 0
				}

				// if missing beacons N times in a row, declare the replica dead
				if r.beaconMisses[q] >= BEACON_FAILURE_THRESHOLD {
					/* Note: check r.Alive[q] is not strictly necessary
					* Checking r.Alive[q] helps avoid checking the inner block in future for dead replica
					* Alternatively, we don't need to check. No checking r.Alive[q] has a benefit:
					* if the view change did not succeed the first time (due to duelling proposal) and
					* no new pilot has been elected, the inner block will try doing view change again.
					 */
					// Approach 1: start vc the first time when we detect alive->dead
					//if r.Alive[q] {
					//	fmt.Printf("Replica %v: detect replica %v dead\n", r.Id, q)
					//	// Start view change for pilot roles which this replica holds
					//	// TODO: should have a map from replicaId to pilot indices
					//	for i := 0; i < NUM_LEADERS; i++ {
					//		// prefer letting non-pilot trying to become a new pilot
					//		if r.views[i].view.ReplicaId == q && !r.IsLeader1 && !r.IsLeader2 {
					//			r.startViewChange(int32(i))
					//			break /*do 1 view change at a time*/
					//		}
					//	}
					//}

					// Approach 2
					if r.Alive[q] {
						fmt.Printf("Replica %v: detect replica %v dead\n", r.Id, q)
					}
					// Start view change for pilot roles which this replica holds
					// TODO: should have a map from replicaId to pilot indices
					for i := 0; i < NUM_LEADERS; i++ {
						// prefer letting non-pilot trying to become a new pilot
						if r.views[i].view.ReplicaId == q && !r.IsLeader1 && !r.IsLeader2 {
							r.startViewChange(int32(i))
							break /*do 1 view change at a time*/
						}
					}

					// reset miss counter to avoid keeping increasing it forever
					r.beaconMisses[q] = 0
					r.Alive[q] = false
				}
			}
			/*if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-beacon takes %v\n", r.Id, time.Since(st))
			}*/

		case <-r.OnClientConnect:
			log.Printf("weird %d; conflicted %d; slow %d; happy %d\n", weird, conflicted, slow, happy)
			weird, conflicted, slow, happy = 0, 0, 0, 0

		case iid := <-r.instancesToRecover:
			st := time.Now()
			r.startRecoveryForInstance(iid.replica, iid.instance)
			if (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= LATENCY_THRESHOLD {
				fmt.Printf("Replica %v: case-iid takes %v\n", r.Id, time.Since(st))
			}

		case slowPathInst := <-r.slowPathChan:
			r.runSlowPath(slowPathInst.replica, slowPathInst.instance)

		case client := <-r.RegisterClientIdChan:
			r.registerClient(client.ClientId, client.Reply)
		/*ok := r.registerClient(client.ClientId, client.Reply)
		rciReply := &genericsmrproto.RegisterClientIdReply{ok}
		r.ReplyRegisterClientId(rciReply, client.Reply)*/

		case getView := <-r.GetViewChan:
			r.handleGetViewFromClient(getView)

		//case <-gcTicker.C:
		//	var garC debug.GCStats
		//	debug.ReadGCStats(&garC)
		//	/*fmt.Printf("\nPauseQuantiles:\t%s", garC.PauseQuantiles) // pause quantiles*/
		//	fmt.Printf("NumGC: %v; PauseTotal: %v; Pause: %v; LastGC: %v\n", garC.NumGC, garC.PauseTotal, garC.Pause, garC.LastGC)
		//	//fmt.Printf("Replica %v: preAcceptReply chan size: %v; proposal chan: %v\n", r.Id, len(r.preAcceptReplyChan), len(r.ProposeChan))
		//	break
		default:

		}

	}
}

/***********************************
   Command execution thread        *
************************************/

type BatchExecLatency struct {
	start  time.Time
	wait   time.Time
	execed time.Time
}

var execLat [][]*BatchExecLatency

func (r *Replica) executeCommands() {

	var TAKEOVER_TIMEOUT = 30 * time.Second
	initTakeoverTimeout := false
	initTakeoverTimer := time.NewTimer(35 * time.Second)

	const SLEEP_TIME_NS = 1 * 1000 // 1 microsecond

	// sanity check the dep invariant
	maxCommittedDeps := make([]int32, NUM_LEADERS)
	maxCommittedDeps[0] = -1
	maxCommittedDeps[1] = -1

	m := make([][]bool, 2)
	m[0] = make([]bool, 2*1024*1024)
	m[1] = make([]bool, 2*1024*1024)

	stats := make([]ExecStats, 2)
	stats[0] = ExecStats{0, 0, 0, 0, 0}
	stats[1] = ExecStats{0, 0, 0, 0, 0}

	nullStarts := make([]int32, 2)
	nullStarts[0] = -1
	nullStarts[1] = -1

	lastPrinted := make([]int32, 2)
	lastPrinted[0] = 0
	lastPrinted[1] = 0

	lastInstWaitingDep := make([]int32, 2)
	lastInstWaitingDep[0] = -1
	lastInstWaitingDep[1] = -1

	if RECORD_EXEC_STATS {
		execLat = make([][]*BatchExecLatency, 2)
		execLat[0] = make([]*BatchExecLatency, 2*1024*1024)
		execLat[1] = make([]*BatchExecLatency, 2*1024*1024)
	}

	var timer05ms *time.Timer
	var timer1ms *time.Timer
	var timer2ms *time.Timer
	var timer5ms *time.Timer
	var timer10ms *time.Timer
	var timer20ms *time.Timer
	var timer40ms *time.Timer
	var timer80ms *time.Timer
	if INJECT_SLOWDOWN {
		timer05ms = time.NewTimer(48 * time.Second)
		timer1ms = time.NewTimer(49 * time.Second)
		timer2ms = time.NewTimer(50 * time.Second)
		timer5ms = time.NewTimer(51 * time.Second)
		timer10ms = time.NewTimer(52 * time.Second)
		timer20ms = time.NewTimer(53 * time.Second)
		timer40ms = time.NewTimer(54 * time.Second)
		timer80ms = time.NewTimer(55 * time.Second)
	}
	allFired := false

	for !r.Shutdown {
		executed := false

		if !initTakeoverTimeout {
			select {
			case <-initTakeoverTimer.C:
				TAKEOVER_TIMEOUT = COMMIT_GRACE_PERIOD
				initTakeoverTimeout = true
			default:
				break
			}
		}

		// if r.IsActive && INJECT_SLOWDOWN && !allFired {
		if r.IsLeader1 && INJECT_SLOWDOWN && !allFired {
			select {
			case <-timer05ms.C:
				fmt.Printf("Replica %v: ExecTimer 0.5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(500 * time.Microsecond)

			case <-timer1ms.C:
				fmt.Printf("Replica %v: ExecTimer 1ms fired at %v\n", r.Id, time.Now())
				time.Sleep(1 * time.Millisecond)

			case <-timer2ms.C:
				fmt.Printf("Replica %v: ExecTimer 2ms fired at %v\n", r.Id, time.Now())
				time.Sleep(2 * time.Millisecond)

			case <-timer5ms.C:
				fmt.Printf("Replica %v: ExecTimer 5ms fired at %v\n", r.Id, time.Now())
				time.Sleep(5 * time.Millisecond)

			case <-timer10ms.C:
				fmt.Printf("Replica %v: ExecTimer 10ms fired at %v\n", r.Id, time.Now())
				time.Sleep(10 * time.Millisecond)

			case <-timer20ms.C:
				fmt.Printf("Replica %v: ExecTimer 20ms fired at %v\n", r.Id, time.Now())
				time.Sleep(20 * time.Millisecond)

			case <-timer40ms.C:
				fmt.Printf("Replica %v: ExecTimer 40ms fired at %v\n", r.Id, time.Now())
				time.Sleep(40 * time.Millisecond)

			case <-timer80ms.C:
				fmt.Printf("Replica %v: ExecTimer 80ms fired at %v\n", r.Id, time.Now())
				allFired = true
				time.Sleep(80 * time.Millisecond)

			default:
				break

			}
		}

		dlog.Println("Replica", r.Id, ": committed up to for leader 0 and 1:", r.CommittedUpTo[0], r.CommittedUpTo[1],
			"; executed up to for leader 0 and 1:", r.ExecedUpTo[0], r.ExecedUpTo[1],
			"; current instance for leader 0 and 1:", r.crtInstance[0], r.crtInstance[1])
		for q := 0; q < NUM_LEADERS; q++ {

			inst := int32(0)

			for inst = r.ExecedUpTo[q] + 1; inst < r.crtInstance[q]; inst++ {

				// Print stats
				if PRINT_STATS && (r.IsLeader1 || r.IsLeader2) && inst > 0 && inst%100000 == 0 && lastPrinted[q] != inst {
					//if  (r.IsLeader1 || r.IsLeader2) && inst > 0 && inst%10000 == 0 && lastPrinted[q] != inst {
					fmt.Println("Replica", r.Id, ": committed up to for leader 0 and 1:", r.CommittedUpTo[0], r.CommittedUpTo[1],
						"; executed up to for leader 0 and 1:", r.ExecedUpTo[0], r.ExecedUpTo[1],
						"; current instance for leader 0 and 1:", r.crtInstance[0], r.crtInstance[1])
					//fmt.Printf("Replica %v: Pilot %v: nullStrikes=%v; nullDepOKs=%v (%v%%); depExced=%v; priority=%v; depWaits=%v (%v%%)\n", r.Id, q, stats[q].numNullDepStrike, stats[q].nullDepOKs, stats[q].nullDepOKs * 100 / uint64(inst), stats[q].depExeced, stats[q].priority, stats[q].depWaits, stats[q].depWaits * 100 / uint64(inst))
					if stats[q].numNullDepStrike > 0 {
						fmt.Printf("Replica %v: Pilot %v: nullStrikes=%v; nullDepOKs=%v; avg. nullDeps/strike=%v\n", r.Id, q, stats[q].numNullDepStrike, stats[q].nullDepOKs, stats[q].nullDepOKs/stats[q].numNullDepStrike)
					}
					lastPrinted[q] = inst
				}

				// Case 0: Current instance is nil.
				// This usually cannot happen. Maybe during leader election? safety check anyway

				if r.InstanceSpace[q][inst] == nil {
					dlog.Println("Replica", r.Id, ": nil instance (L,i)", q, inst)
					break
				}

				currInst := r.InstanceSpace[q][inst]

				// Update maxCommittedDeps
				if currInst.Status >= latentcopilotproto.COMMITTED && maxCommittedDeps[q] < currInst.Deps[0] {
					maxCommittedDeps[q] = currInst.Deps[0]
				}
				// Case 1: Current instance was executed
				if currInst.Status == latentcopilotproto.EXECUTED {
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}

					continue
				}

				// Case 2: Current instance is committed but not yet executed
				if currInst.Status == latentcopilotproto.COMMITTED {
					//_, exists := execLat[q][inst]
					if RECORD_EXEC_STATS && (r.IsLeader1 || r.IsLeader2) && execLat[q][inst] == nil {
						execLat[q][inst] = &BatchExecLatency{time.Now(), time.Time{}, time.Time{}}
					}

					dep := currInst.Deps[0]

					// Case 2.1: no dependency (dep = -1) or the dependency was executed
					if dep == -1 || (r.InstanceSpace[1-q][dep] != nil && (r.InstanceSpace[1-q][dep].Status == latentcopilotproto.EXECUTED || r.ExecedUpTo[1-q] >= dep)) {
						if ok := r.executeBatch(q, inst); ok {
							executed = true
							currInst.Status = latentcopilotproto.EXECUTED
							if inst == r.ExecedUpTo[q]+1 {
								r.ExecedUpTo[q] = inst
							}

							continue

						}
					} // end case 2.1: dependency was executed

					allDepsNullable := currInst.nullDepSafe && r.views[1-q].view.ViewId >= currInst.depViewId && r.checkAllDepsExecuted(1-q, r.ExecedUpTo[1-q]+1, dep)
					if ADAPTIVE_TIMEOUT_ENABLED {
						if r.Id == int32(q) {
							if allDepsNullable {
								adaptedPingPongTimeout = BATCH_INTERVAL
							} else {
								adaptedPingPongTimeout = PINGPONG_TIMEOUT
							}
						}
					}
					if allDepsNullable {
						if nullStarts[q] == -1 {
							nullStarts[q] = inst
						}
					} else if nullStarts[q] != -1 {
						stats[q].numNullDepStrike++
						nullStarts[q] = -1
					}
					if allDepsNullable {

						// update stats
						if allDepsNullable {
							stats[q].nullDepOKs++
						} else {
							stats[q].depExeced++
						}

						if ok := r.executeBatch(q, inst); ok {
							executed = true
							currInst.Status = latentcopilotproto.EXECUTED
							if inst == r.ExecedUpTo[q]+1 {
								r.ExecedUpTo[q] = inst
							}

							continue

						}
					} // end case : dependency was nullable

					depInst := r.InstanceSpace[1-q][dep]

					// Case 2.2: the dependency is committed but not yet executed
					if depInst != nil && depInst.Status == latentcopilotproto.COMMITTED {

						// can execute if
						// (1) maxCommittedDeps[1-q] >= inst and q has higher priority or
						// (2) depInst.Deps[0] >= inst and q has higher priority AND all batches before dep was executed
						if (maxCommittedDeps[1-q] >= inst && q == HIGH_PRIORITY_LEADER) ||
							(depInst.Deps[0] >= inst && q == HIGH_PRIORITY_LEADER && r.ExecedUpTo[1-q] >= dep-1) {

							// update stats
							stats[q].priority++

							if ok := r.executeBatch(q, inst); ok {
								dlog.Println("Replica", r.Id, ": instance (L,i) is committed and has higher priority", q, inst)
								executed = true
								currInst.Status = latentcopilotproto.EXECUTED
								if inst == r.ExecedUpTo[q]+1 {
									r.ExecedUpTo[q] = inst
								}

								continue
							}
						}

						// check if we need to do fast takeover for any instance before dependency
						/* note: use the commented line below if a pilot wants to do re-takeover of its own entries */
						//if (r.IsLeader1 || r.IsLeader2) && time.Since(currInst.committedTime) >= TAKEOVER_TIMEOUT && time.Since(currInst.committedTime) <= 180*time.Second {
						if ((r.IsLeader1 && q == 0) || (r.IsLeader2 && q == 1)) && time.Since(currInst.committedTime) >= TAKEOVER_TIMEOUT && time.Since(currInst.committedTime) <= 180*time.Second {
							for pi := r.CommittedUpTo[1-q] + 1; pi < dep; pi++ {
								//if !m[1-q][pi] && (r.InstanceSpace[1-q][pi] == nil || (r.InstanceSpace[1-q][pi].Status != latentcopilotproto.COMMITTED && r.InstanceSpace[1-q][pi].Status != latentcopilotproto.EXECUTED)) {
								if !m[1-q][pi] && (r.InstanceSpace[1-q][pi] != nil && (r.InstanceSpace[1-q][pi].Status != latentcopilotproto.COMMITTED && r.InstanceSpace[1-q][pi].Status != latentcopilotproto.EXECUTED)) {

									r.instancesToRecover <- &instanceId{int32(1 - q), pi}
									m[1-q][pi] = true
								}
							}
						}

						// update stats
						if lastInstWaitingDep[q] != inst {
							stats[q].depWaits++
							lastInstWaitingDep[q] = inst
						}

						if RECORD_EXEC_STATS && (r.IsLeader1 || r.IsLeader2) && time.Time.IsZero(execLat[q][inst].wait) {
							execLat[q][inst].wait = time.Now()
						}

						dlog.Println("Replica", r.Id, ": instance (L,i) is committed", q, inst, "but waiting for dep", dep, ";dep status", depInst.Status, "; dep's dep = ", depInst.Deps[0], "other exec upto", r.ExecedUpTo[1-q])
						break

					} // end case 2.2: the dependency is committed but not yet executed

					// there is a cycle caused by the earlier batches than dep from the same dep's leader
					if maxCommittedDeps[1-q] >= inst && q == HIGH_PRIORITY_LEADER {
						//if (maxCommittedDeps[1-q] >= inst && q == HIGH_PRIORITY_LEADER) || r.ExecedUpTo[1-q] >= dep {
						stats[q].priority++
						if ok := r.executeBatch(q, inst); ok {
							dlog.Println("Replica", r.Id, ": instance (L,i) is committed and has higher priority, although dep's status is unknown", q, inst)
							executed = true
							currInst.Status = latentcopilotproto.EXECUTED
							if inst == r.ExecedUpTo[q]+1 {
								r.ExecedUpTo[q] = inst
							}
							continue
						}
					}

					// Case 2.3: the dependency is not yet committed or nil and there is no cycle caused by earlier batches
					// TODO: Wait and then takeover
					if depInst == nil || (depInst.Status != latentcopilotproto.COMMITTED && depInst.Status != latentcopilotproto.EXECUTED) {
						/* note: use the commented line below if a pilot wants to do re-takeover of its own entries */
						// if (r.IsLeader1 || r.IsLeader2) && r.Id == int32(q) && time.Since(currInst.committedTime) >= TAKEOVER_TIMEOUT && time.Since(currInst.committedTime) <= 180*time.Second {
						if ((r.IsLeader1 && q == 0) || (r.IsLeader2 && q == 1)) && time.Since(currInst.committedTime) >= TAKEOVER_TIMEOUT && time.Since(currInst.committedTime) <= 180*time.Second {

							for pi := r.CommittedUpTo[1-q] + 1; pi <= dep; pi++ {
								if !m[1-q][pi] && (r.InstanceSpace[1-q][pi] != nil && (r.InstanceSpace[1-q][pi].Status != latentcopilotproto.COMMITTED && r.InstanceSpace[1-q][pi].Status != latentcopilotproto.EXECUTED)) {
									r.instancesToRecover <- &instanceId{int32(1 - q), pi}
									m[1-q][pi] = true
								}
							}
						}

						// update stats
						if lastInstWaitingDep[q] != inst {
							stats[q].depWaits++
							lastInstWaitingDep[q] = inst
						}
						if RECORD_EXEC_STATS && (r.IsLeader1 || r.IsLeader2) && time.Time.IsZero(execLat[q][inst].wait) {
							execLat[q][inst].wait = time.Now()
						}

						//fmt.Println("Replica ", r.Id, ": batch (", q, inst, ") is waiting for dep ", dep, "to commit")
						break
					} // end case 2.3: the dependency is not yet committed or nil

				} // end of case 2: Current instance is committed but not yet executed

			} // end loop through instances of a leader
		} // end loop through two leader

		if !executed {
			time.Sleep(1 * SLEEP_TIME_NS)
		}

	}
}

func (r *Replica) checkDepExecuted(leaderId int, instNum int32) bool {

	if instNum < 0 {
		return true
	}
	inst := r.InstanceSpace[leaderId][instNum]
	if inst == nil || inst.Status == latentcopilotproto.NONE {
		return false
	}

	// This is no-op
	if inst.Cmds == nil {
		return true
	}

	//allExist := true
	for j := 0; j < len(inst.Cmds); j++ {
		clientId := inst.Cmds[j].ClientId
		opId := inst.Cmds[j].OpId
		k := getKeyForExecMap(clientId, opId)
		//_, exists := r.execMap[k]
		exists := r.execMap[k]
		//lastOp := r.latestOps[clientId]
		//exists := lastOp >= opId
		if !exists {
			return false
		}
	}
	/*
		for j := 0; j < len(inst.Cmds); j++ {
			clientId := inst.Cmds[j].ClientId
			opId := inst.Cmds[j].OpId
			k := getKeyForExecMap(clientId, opId)
			_, exists := r.execMap[k]
			if exists {
				delete(r.execMap, k)
			}
		}
	*/

	return true
}

func (r *Replica) checkAllDepsExecuted(leaderId int, start, end int32) bool {
	if start < 0 {
		return true
	}
	for i := start; i <= end; i++ {
		if !r.checkDepExecuted(leaderId, i) {
			return false
		}
		/*if r.ExecedUpTo[leaderId]+1 == i {
			r.ExecedUpTo[leaderId] = i
		}*/
	}
	return true
}

func (r *Replica) executeBatch(leaderId int, instNum int32) bool {

	inst := r.InstanceSpace[leaderId][instNum]
	if inst == nil {
		return false
	}

	// No-op
	// if inst.Cmds == nil {
	if len(inst.Cmds) == 0 {
		// log.Printf("Replica %v: empty commands in (%v.%v)\n", r.Id, leaderId, instNum)
		return true
	}

	// Result executed but not replied to client yet
	if RECORD_EXEC_STATS && PRINT_STATS && (r.IsLeader1 || r.IsLeader2) {
		if time.Time.IsZero(execLat[leaderId][instNum].wait) {
			execLat[leaderId][instNum].wait = execLat[leaderId][instNum].start
		}
		execLat[leaderId][instNum].execed = time.Now()
		if execLat[leaderId][instNum].execed.Sub(inst.startTime) >= LATENCY_THRESHOLD {
			fmt.Printf("Replica %v: (%v.%v): commit %v; execStart %v; execWait %v, execed %v\n", r.Id, leaderId, instNum,
				inst.committedTime.Sub(inst.startTime),
				execLat[leaderId][instNum].start.Sub(inst.startTime),
				execLat[leaderId][instNum].wait.Sub(inst.startTime),
				execLat[leaderId][instNum].execed.Sub(inst.startTime),
			)
		}
	}

	var b int64 = int64(instNum)
	// b > 0 means batch |b| from pilot 0
	// b < 0 means batch |b| from pilot 1
	if leaderId == 1 {
		b = -b
	}
	st := time.Now()
	count := 0
	for j := 0; j < len(inst.Cmds); j++ {

		reqSt := time.Now()
		clientId := inst.Cmds[j].ClientId
		opId := inst.Cmds[j].OpId
		k := getKeyForExecMap(clientId, opId)
		//_, exists := r.execMap[k]
		exists := r.execMap[k]
		//lastOp := r.latestOps[clientId]
		//exists := lastOp >= opId

		var val state.Value
		if !exists {
			val = inst.Cmds[j].Execute(r.State)
			// r.clientMutex.Lock()
			r.execMap[k] = true
			// r.clientMutex.Unlock()
			//r.latestOps[clientId] = opId
		} else {
			//delete(r.execMap, k)
			continue
		}
		if PRINT_STATS && (r.IsLeader1 || r.IsLeader2) && time.Since(reqSt) >= 5*time.Millisecond {
			fmt.Printf("Replica %v executed request %v-%v in (%v.%v): started at %v; took %v; mapsize %v\n", r.Id, inst.Cmds[j].ClientId, opId, leaderId, instNum, reqSt, time.Since(reqSt), len(r.execMap))
		}

		if !r.IsLeader1 && !r.IsLeader2 && !r.replicaReply {
			return true
		}

		count++
		//reqSt := time.Now()
		if writer, ok := r.clientWriters[inst.Cmds[j].ClientId]; ok {
			propreply := &genericsmrproto.ProposeReplyTS{
				TRUE,
				opId,
				val,
				//int64(0)}
				//int64(instNum)}
				b} /*instance number*/
			r.ReplyProposeTS(propreply, writer)
			//r.execMap[k] = true
			//r.latestOps[clientId] = opId
		} else if inst.lb != nil && inst.lb.clientProposals != nil {
			//r.execMap[k] = true
			//r.latestOps[clientId] = opId
			propreply := &genericsmrproto.ProposeReplyTS{
				TRUE,
				inst.lb.clientProposals[j].Command.OpId,
				val,
				inst.lb.clientProposals[j].Timestamp}
			r.ReplyProposeTS(propreply, inst.lb.clientProposals[j].Reply)
		}
		if PRINT_STATS && (r.IsLeader1 || r.IsLeader2) && time.Since(reqSt) >= 5*time.Millisecond {
			fmt.Printf("Replica %v sending replies to %v-%v in (%v.%v): started at %v; took %v; mapsize %v\n", r.Id, inst.Cmds[j].ClientId, opId, leaderId, instNum, reqSt, time.Since(reqSt), len(r.execMap))
		}
	}

	if PRINT_STATS && (r.IsLeader1 || r.IsLeader2) && time.Since(st) >= 5*time.Millisecond {
		fmt.Printf("Replica %v sending %v/%v replies for (%v.%v): started at %v; took %v; mapsize %v\n", r.Id, count, len(inst.Cmds), leaderId, instNum, st, time.Since(st), len(r.execMap))
	}
	if r.IsLeader1 || r.IsLeader2 {
		dlog.Println("Replica ", r.Id, ": successfully executed (l,b,d) = ", leaderId, instNum, inst.Deps[0])
	}

	return true
}

func getKeyForExecMap(clientId uint32, opId int32) uint32 {
	return (clientId << 21) | uint32(opId)
}

/* Ballot helper functions */
func (r *Replica) makeUniqueViewId(counter int32) int32 {
	return (counter << REPLICA_ID_NBITS) | r.Id
}

func (r *Replica) makeViewIdLargerThan(viewId int32) int32 {
	return r.makeUniqueViewId((viewId >> REPLICA_ID_NBITS) + 1)
}

func (r *Replica) makeBallotLargerThan(ballot int32) int32 {
	return makeBallot(subballotFromBallot(ballot)+1, originalProposerIdFromBallot(ballot), r.Id)
}

func isInitialBallot(ballot int32) bool {
	return subballotFromBallot(ballot) == 0
}

func subballotFromBallot(ballot int32) int32 {
	return ballot >> (REPLICA_ID_NBITS + REPLICA_ID_NBITS)
}

func replicaIdFromBallot(ballot int32) int32 {
	return ballot & REPLICA_ID_BITMASK
}

func originalProposerIdFromBallot(ballot int32) int32 {
	return (ballot >> REPLICA_ID_BITMASK) & REPLICA_ID_BITMASK
}

// last 4 bit: replica Id
// next to last 4 bit: replica Id of the original proposer
// the rest: sub-ballot
func makeBallot(subballot, originalProposerId, replicaId int32) int32 {
	return (subballot << (REPLICA_ID_NBITS + REPLICA_ID_NBITS)) | (originalProposerId << REPLICA_ID_NBITS) | replicaId
}

/**********************************************************************
                    inter-replica communication
***********************************************************************/

func (r *Replica) replyPrepare(replicaId int32, reply *latentcopilotproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyPreAccept(replicaId int32, reply *latentcopilotproto.PreAcceptReply) {
	r.SendMsg(replicaId, r.preAcceptReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *latentcopilotproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyTryPreAccept(replicaId int32, reply *latentcopilotproto.TryPreAcceptReply) {
	r.SendMsg(replicaId, r.tryPreAcceptReplyRPC, reply)
}

func (r *Replica) bcastPrepare(replica int32, instance int32, viewId int32, ballot int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	args := &latentcopilotproto.Prepare{r.Id, replica, instance, viewId, ballot}

	n := r.N - 1
	/*if r.Thrifty {
		n = r.N / 2 + 1
	}*/
	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			dlog.Println("Not enough replicas alive!")
			break
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.prepareRPC, args)
		sent++
	}
}

var pa latentcopilotproto.PreAccept

func (r *Replica) bcastPreAccept(replica int32, instance int32, viewId int32, ballot int32, cmds []state.Command, deps []int32, depViewId int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Replica = replica
	pa.ViewId = viewId
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = cmds
	pa.Deps = deps
	pa.DepViewId = depViewId
	args := &pa

	n := r.N - 1
	if r.Thrifty {
		n = r.N/2 + (r.N/2+1)/2
	}

	sent := 0
	if !r.Thrifty {
		for q := 0; q < r.N-1; q++ {
			if !r.Alive[r.PreferredPeerOrder[q]] {
				continue
			}
			//dlog.Println("Replica ", replica, " is sending fast accept (", instance, deps[0], ") to ", r.PreferredPeerOrder[q])
			r.SendMsg(r.PreferredPeerOrder[q], r.preAcceptRPC, args)
			sent++
			if sent >= n {
				break
			}
		}
	} else {
		for q := 0; q < r.N; q++ {
			if int32(q) == r.Id || !r.Alive[q] {
				continue
			}
			//dlog.Println("Replica ", replica, " is sending fast accept (", instance, deps[0], ") to ", r.PreferredPeerOrder[q])
			r.SendMsg(int32(q), r.preAcceptRPC, args)
			sent++
			if sent >= n {
				break
			}
		}
	}
	r.stat.rpcsSent += uint64(n)
}

var tpa latentcopilotproto.TryPreAccept

func (r *Replica) bcastTryPreAccept(replica int32, instance int32, ballot int32, cmds []state.Command, deps []int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept bcast failed:", err)
		}
	}()
	tpa.LeaderId = r.Id
	tpa.Replica = replica
	tpa.Instance = instance
	tpa.Ballot = ballot
	tpa.Command = cmds
	tpa.Deps = deps
	args := &tpa

	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.tryPreAcceptRPC, args)
	}
}

func (r *Replica) bcastAccept(replica int32, instance int32, viewId int32, ballot int32, command []state.Command, deps []int32, depViewId int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Accept bcast failed:", err)
		}
	}()

	var ea latentcopilotproto.Accept

	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.ViewId = viewId
	ea.Ballot = ballot
	ea.Command = command
	ea.Deps = deps
	ea.DepViewId = depViewId
	args := &ea

	n := r.N - 1
	if r.Thrifty {
		n = r.N/2 + 1
	}

	sent := 0
	if !r.Thrifty {
		for q := 0; q < r.N-1; q++ {
			if !r.Alive[r.PreferredPeerOrder[q]] {
				continue
			}
			r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
			sent++
			if sent >= n {
				break
			}
		}
	} else {
		for q := 0; q < r.N; q++ {
			if int32(q) == r.Id || !r.Alive[q] {
				continue
			}
			r.SendMsg(int32(q), r.acceptRPC, args)
			sent++
			if sent >= n {
				break
			}
		}
	}
	r.stat.rpcsSent += uint64(n)
}

func (r *Replica) bcastCommit(replica int32, instance int32, cmds []state.Command, deps []int32, nullDepSafe bool, depViewId int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Commit bcast failed:", err)
		}
	}()

	var ec latentcopilotproto.Commit
	var ecs latentcopilotproto.CommitShort
	nds := bool2uint8(nullDepSafe)

	ec.LeaderId = r.Id
	ec.Replica = replica
	ec.Instance = instance
	ec.Command = cmds
	ec.Deps = deps
	ec.NullDepSafe = nds
	ec.DepViewId = depViewId
	args := &ec

	dlog.Printf("Replica %d: sending commit (%d.%d, %d)\n", r.Id, replica, instance, deps[0])

	ecs.LeaderId = r.Id
	ecs.Replica = replica
	ecs.Instance = instance
	ecs.Count = int32(len(cmds))
	ecs.Deps = deps
	ecs.NullDepSafe = nds
	ecs.DepViewId = depViewId
	argsShort := &ecs

	sent := 0

	if !r.Thrifty {
		for q := 0; q < r.N-1; q++ {
			peer := r.PreferredPeerOrder[q]
			//if !r.Alive[r.PreferredPeerOrder[q]] {
			if !r.Alive[peer] || peer == r.Id {
				continue
			}

			//r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, args)

			//if r.Thrifty && sent >= r.N/2 {
			if (r.Thrifty && sent >= r.N/2) || cmds == nil {
				r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, args)
			} else {
				//r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
				r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, args)
				sent++
			}
		}
	} else {
		for q := 0; q < r.N; q++ {
			peer := int32(q)
			if peer == r.Id || !r.Alive[peer] {
				continue
			}
			if r.Thrifty && sent >= r.N/2 {
				r.SendMsg(peer, r.commitRPC, args)
			} else {
				r.SendMsg(peer, r.commitShortRPC, argsShort)
				sent++
			}
		}
	}
	r.stat.rpcsSent += uint64(r.N - 1)
}

/******************************************************************
               Helper functions
*******************************************************************/

func (r *Replica) clearHashtables() {
	for q := 0; q < r.N; q++ {
		r.conflicts[q] = make(map[state.Key]int32, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommittedMap(cmds []state.Command) {
	for _, cmd := range cmds {
		r.committedMap[getKeyForExecMap(cmd.ClientId, cmd.OpId)] = true
	}
}

func (r *Replica) updateCommitted(replica int32) {
	/*if r.CommittedUpTo[replica] < r.ExecedUpTo[replica] {
		r.CommittedUpTo[replica] = r.ExecedUpTo[replica]
	}*/
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == latentcopilotproto.COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == latentcopilotproto.EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
	}
}

func equal(deps1 []int32, deps2 []int32) bool {
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

func bfFromCommands(cmds []state.Command) *bloomfilter.Bloomfilter {
	if cmds == nil {
		return nil
	}

	bf := bloomfilter.NewPowTwo(bf_PT, BF_K)

	for i := 0; i < len(cmds); i++ {
		bf.AddUint64(uint64(cmds[i].K))
	}

	return bf
}

// check if the proposed (batch, dep) has any ordering conflict with the other leader
// return true if there is conflict
// return false otherwise
// 3rd return: true if this replica has seen dep, false otherwise
func (r *Replica) checkConflicts(thisLeader int32, instance int32, deps []int32, otherLeader int32) (bool, []int32, bool) {

	// latest instance from other leader known to this replica
	otherLeaderCrtInstance := r.crtInstance[otherLeader] - 1
	dep := deps[0]

	// no conflict, seen dep
	if dep == otherLeaderCrtInstance {
		return false, deps, true
	}

	// no conflict, not seen dep yet
	if dep > otherLeaderCrtInstance {
		return false, deps, false
	}

	newDeps := make([]int32, DS)
	for i := 0; i < DS; i++ {
		newDeps[i] = deps[i]
	}

	// condition for a conflict:
	// If there exists a batch (i) from the other leader, which is later than dep and whose (i's) dep
	// is earlier than instance
	for i := otherLeaderCrtInstance; i > dep; i-- {
		if r.InstanceSpace[otherLeader][i] != nil {
			//iDep := r.InstanceSpace[otherLeader][i].Deps[0]
			iDep := r.preAcceptDeps[otherLeader][i]
			if iDep == -1 || iDep < instance {
				newDeps[0] = i
				return true, newDeps, true
			}
		} else {
			// this should not happen if we enforce the FIFO
		}
	}

	return false, deps, true
}

func (r *Replica) getDependency(otherLeaderId int32) []int32 {

	deps := make([]int32, DS)

	for q := 0; q < DS; q++ {
		deps[q] = -1
	}

	otherLeaderCrtInstance := r.crtInstance[otherLeaderId]
	for otherLeaderCrtInstance >= 0 && r.InstanceSpace[otherLeaderId][otherLeaderCrtInstance] == nil {
		otherLeaderCrtInstance--
	}
	deps[0] = otherLeaderCrtInstance

	return deps
}

func (r *Replica) mergeDependency(deps1 []int32, deps2 []int32) ([]int32, bool) {
	equal := true

	if deps1[0] != deps2[0] {
		equal = false
		if deps2[0] > deps1[0] {
			deps1[0] = deps2[0]
		}
	}

	return deps1, equal
}

func (r *Replica) startFastPathClock(iid *instanceId) {
	// TODO: should adapt this timeout; and should change to larger value for wide-area
	time.Sleep(FAST_PATH_TIMEOUT)
	inst := r.InstanceSpace[iid.replica][iid.instance]
	if inst == nil || (inst.Status != latentcopilotproto.PREACCEPTED && inst.Status != latentcopilotproto.PREACCEPTED_EQ) ||
		(inst.lb.preAcceptOKs+inst.lb.preAcceptReplies) < r.N/2 {
		return
	}
	r.slowPathChan <- iid
}

/**********************************************************************

                            PHASE 1

***********************************************************************/
func (r *Replica) handleProposeForLatentPilot(propose *genericsmr.Propose) {
	//TODO!! Handle client retries

	if !r.IsLeader1 && !r.IsLeader2 {
		return
	}

	numProposals := len(r.onOffProposeChanReal) + 1
	batchSize := len(r.onOffProposeChanReal) + 1
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}
	cmds := make([]state.Command, 0, batchSize)
	proposals := make([]*genericsmr.Propose, 0, batchSize)
	k := getKeyForExecMap(propose.Command.ClientId, propose.CommandId)
	// TODO: consider having committed map for command instead of using execMap
	if !r.committedMap[k] {
		cmds = append(cmds, propose.Command)
		proposals = append(proposals, propose)
	} /*else {
		r.clientMutex.Lock()
		execed := r.execMap[k]
		r.clientMutex.Unlock()
		if execed {
			propreply := &genericsmrproto.ProposeReplyTS{
				TRUE,
				propose.Command.OpId,
				propose.Command.V,
				propose.Timestamp}
			r.ReplyProposeTS(propreply, propose.Reply)
		}
	}*/

	for i := 1; i < numProposals; i++ {
		prop := <-r.onOffProposeChanReal
		k = getKeyForExecMap(prop.Command.ClientId, prop.CommandId)
		if !r.committedMap[k] {
			cmds = append(cmds, prop.Command)
			proposals = append(proposals, prop)
			if len(cmds) == MAX_BATCH {
				break
			}
		} /*else {
			r.clientMutex.Lock()
			execed := r.execMap[k]
			r.clientMutex.Unlock()
			if execed {
				propreply := &genericsmrproto.ProposeReplyTS{
					TRUE,
					propose.Command.OpId,
					propose.Command.V,
					propose.Timestamp}
				r.ReplyProposeTS(propreply, propose.Reply)
			}
		}*/
	}
	if len(cmds) == 0 {
		return
	}

	batchSize = len(cmds)

	if PRINT_STATS && r.stat.batches%100000 == 0 {
		log.Printf("Replica %d: batches: %v, cmds: %v, avg. cmds/batch: %.2f, "+
			"fast: %v, slow: %v,"+
			"rpcsSent: %v, rpcsRcvd: %v, total rpcs: %v, avg. rpcs/batch: %.2f, avg. rpcs/cmd: %.2f"+
			"\n",
			r.Id, r.stat.batches, r.stat.cmds, float64(r.stat.cmds)/float64(r.stat.batches),
			r.stat.fast, r.stat.slow,
			r.stat.rpcsSent, r.stat.rpcsRcvd, r.stat.rpcsSent+r.stat.rpcsRcvd,
			float64(r.stat.rpcsSent+r.stat.rpcsRcvd-2*r.stat.cmds)/float64(r.stat.batches),
			float64(r.stat.rpcsSent+r.stat.rpcsRcvd)/float64(r.stat.cmds))
	}

	r.stat.batches++
	r.stat.cmds += uint64(batchSize)
	// rpcs from client requests (receive and send replies)
	r.stat.rpcsRcvd += uint64(batchSize)
	r.stat.rpcsSent += uint64(batchSize)

	var pilotId int32 = 0
	if !r.IsLeader1 {
		pilotId = 1
	}
	instNo := r.crtInstance[pilotId]
	r.crtInstance[pilotId]++

	dlog.Printf("Starting instance %d\n", instNo)
	dlog.Printf("Batching %d\n", batchSize)

	if PRINT_STATS && (time.Now().UnixNano()-propose.Timestamp) >= int64(5000000) /*5ms*/ {
		fmt.Printf("Replica %v: PROPOSE request %v-%v in (%v.%v) takes %v (us)\n", r.Id, propose.Command.ClientId, propose.CommandId, r.Id, instNo, (time.Now().UnixNano()-propose.Timestamp)/int64(1000))
	}

	r.startPhase1ForLatent(pilotId, instNo, r.views[pilotId].defaultBallot, proposals, cmds, batchSize)
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	//TODO!! Handle client retries

	if !r.IsLeader1 && !r.IsLeader2 {
		return
	}
	batchSize := len(r.ProposeChan) + 1
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	if PRINT_STATS && r.stat.batches%100000 == 0 {
		log.Printf("Replica %d: batches: %v, cmds: %v, avg. cmds/batch: %.2f, "+
			"fast: %v, slow: %v,"+
			"rpcsSent: %v, rpcsRcvd: %v, total rpcs: %v, avg. rpcs/batch: %.2f, avg. rpcs/cmd: %.2f"+
			"\n",
			r.Id, r.stat.batches, r.stat.cmds, float64(r.stat.cmds)/float64(r.stat.batches),
			r.stat.fast, r.stat.slow,
			r.stat.rpcsSent, r.stat.rpcsRcvd, r.stat.rpcsSent+r.stat.rpcsRcvd,
			float64(r.stat.rpcsSent+r.stat.rpcsRcvd-2*r.stat.cmds)/float64(r.stat.batches),
			float64(r.stat.rpcsSent+r.stat.rpcsRcvd)/float64(r.stat.cmds))
	}

	r.stat.batches++
	r.stat.cmds += uint64(batchSize)
	// rpcs from client requests (receive and send replies)
	r.stat.rpcsRcvd += uint64(batchSize)
	r.stat.rpcsSent += uint64(batchSize)

	var pilotId int32 = 0
	if !r.IsLeader1 {
		pilotId = 1
	}
	instNo := r.crtInstance[pilotId]
	r.crtInstance[pilotId]++

	dlog.Printf("Starting instance %d\n", instNo)
	dlog.Printf("Batching %d\n", batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	if PRINT_STATS && (time.Now().UnixNano()-propose.Timestamp) >= int64(5000000) /*5ms*/ {
		fmt.Printf("Replica %v: PROPOSE request %v-%v in (%v.%v) takes %v (us)\n", r.Id, propose.Command.ClientId, propose.CommandId, r.Id, instNo, (time.Now().UnixNano()-propose.Timestamp)/int64(1000))

	}

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
		if PRINT_STATS && (time.Now().UnixNano()-prop.Timestamp) >= int64(5000000) /*5ms*/ {
			fmt.Printf("Replica %v: PROPOSE request %v-%v in (%v.%v) takes %v (us)\n", r.Id, prop.Command.ClientId, prop.CommandId, r.Id, instNo, (time.Now().UnixNano()-prop.Timestamp)/int64(1000))
		}
	}

	// TODO: for ballot, may need to use default ballot with view number
	r.startPhase1(pilotId, instNo, r.views[pilotId].defaultBallot, proposals, cmds, batchSize)
}

func (r *Replica) startPhase1ForLatent(replica int32, instance int32, ballot int32, proposals []*genericsmr.Propose, cmds []state.Command, batchSize int) {

	// 1. Check which pilot this replica is (e.g., pilot 0/1/2...). This pilot id is different from replica id
	// 2. Use pilot id as index to get view state. if v/c in progress, should not propose.
	// (Should be aware of the case where a replica acts as multiple pilots)
	if !r.IsLeader1 && !r.IsLeader2 {
		return
	}

	viewId := r.views[replica].view.ViewId
	deps := make([]int32, DS)
	for q := 0; q < DS; q++ {
		deps[q] = -1
	}

	var otherLeaderId int32 = 0
	if r.IsLeader1 {
		otherLeaderId = 1
	} else {
		otherLeaderId = 0
	}
	deps = r.getDependency(otherLeaderId)

	comDeps := make([]int32, DS)
	for i := 0; i < DS; i++ {
		comDeps[i] = -1
	}

	preAcceptReplyDeps := make([]int32, 1, r.N)
	preAcceptReplyDeps[0] = deps[0]
	depViewId := r.views[otherLeaderId].view.ViewId

	// Update preAcceptedDeps this replica promised for the sender
	r.preAcceptDeps[replica][instance] = deps[0]

	r.InstanceSpace[replica][instance] = &Instance{
		cmds,
		ballot,
		latentcopilotproto.PREACCEPTED_EQ,
		deps,
		&LeaderBookkeeping{proposals, 0, 0, true, 0, 0, preAcceptReplyDeps, 0, 0, deps, comDeps, nil, false, false, nil, 0, 1, depViewId, false}, 0, 0,
		nil, time.Now(), time.Time{}, false, depViewId, -1}

	r.recordInstanceMetadata(r.InstanceSpace[replica][instance])
	r.recordCommands(cmds)
	r.sync()

	// we're on the latent path, run fast takeover for uncommitted deps
	//if warmupDone {
	// when in latent, it has not done fast takeover yet
	// it only does fast takeover for late batches after it is promoted to active
	if r.IsActive {
		if r.latestTakeover[otherLeaderId] < r.CommittedUpTo[otherLeaderId] {
			r.latestTakeover[otherLeaderId] = r.CommittedUpTo[otherLeaderId]
		}
		for i := r.latestTakeover[otherLeaderId] + 1; i <= deps[0]; i++ {
			depInst := r.InstanceSpace[otherLeaderId][i]
			if depInst != nil && depInst.Status < latentcopilotproto.COMMITTED {
				r.instancesToRecover <- &instanceId{otherLeaderId, i}
				r.latestTakeover[otherLeaderId] = i
			}
		}
	}
	// step up being an active pilot
	/*if !r.IsActive && time.Since(r.activeStartTime) > SWITCHING_MODE_WINDOW {
		r.IsActive = true
		r.activeStartTime = time.Now()
		fmt.Printf("Replica %v: becomes ACTIVE at %v\n", r.Id, time.Now())
	}*/

	dlog.Printf("Replica %d is sending PreAccept for (%d.%d, %d):\n", r.Id, r.Id, instance, deps[0])
	r.bcastPreAccept(replica, instance, viewId, ballot, cmds, deps, depViewId)
	//go r.startFastPathClock(&instanceId{r.Id, instance})

	cpcounter += batchSize

	if r.Id == 0 && DO_CHECKPOINTING && cpcounter >= CHECKPOINT_PERIOD {
		cpcounter = 0

		//Propose a checkpoint command to act like a barrier.
		//This allows replicas to discard their dependency hashtables.
		r.crtInstance[r.Id]++
		instance++

		for q := 0; q < r.N; q++ {
			deps[q] = r.crtInstance[q] - 1
		}

		r.InstanceSpace[r.Id][instance] = &Instance{
			cpMarker,
			0,
			//latentcopilotproto.PREACCEPTED,
			latentcopilotproto.PREACCEPTED_EQ,
			deps,
			&LeaderBookkeeping{nil, 0, 0, true, 0, 0, nil, 0, 0, deps, nil, nil, false, false, nil, 0, 1, depViewId, false},
			0,
			0,
			nil, time.Time{}, time.Time{}, false, -1, -1}

		r.latestCPReplica = r.Id
		r.latestCPInstance = instance

		//discard dependency hashtables
		r.clearHashtables()

		r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
		r.sync()

		r.bcastPreAccept(r.Id, instance, viewId, 0, cpMarker, deps, depViewId)
	}
}

func (r *Replica) startPhase1(replica int32, instance int32, ballot int32, proposals []*genericsmr.Propose, cmds []state.Command, batchSize int) {

	// 1. Check which pilot this replica is (e.g., pilot 0/1/2...). This pilot id is different from replica id
	// 2. Use pilot id as index to get view state. if v/c in progress, should not propose.
	// (Should be aware of the case where a replica acts as multiple pilots)
	if !r.IsLeader1 && !r.IsLeader2 {
		return
	}

	viewId := r.views[replica].view.ViewId
	deps := make([]int32, DS)
	for q := 0; q < DS; q++ {
		deps[q] = -1
	}

	var otherLeaderId int32 = 0
	if r.IsLeader1 {
		otherLeaderId = 1
	} else {
		otherLeaderId = 0
	}
	deps = r.getDependency(otherLeaderId)

	comDeps := make([]int32, DS)
	for i := 0; i < DS; i++ {
		comDeps[i] = -1
	}

	preAcceptReplyDeps := make([]int32, 1, r.N)
	preAcceptReplyDeps[0] = deps[0]
	depViewId := r.views[otherLeaderId].view.ViewId

	// Update preAcceptedDeps this replica promised for the sender
	r.preAcceptDeps[replica][instance] = deps[0]

	r.InstanceSpace[replica][instance] = &Instance{
		cmds,
		ballot,
		//latentcopilotproto.PREACCEPTED,
		latentcopilotproto.PREACCEPTED_EQ,
		deps,
		&LeaderBookkeeping{proposals, 0, 0, true, 0, 0, preAcceptReplyDeps, 0, 0, deps, comDeps, nil, false, false, nil, 0, 1, depViewId, false}, 0, 0,
		nil, time.Now(), time.Time{}, false, depViewId, -1}

	r.recordInstanceMetadata(r.InstanceSpace[replica][instance])
	r.recordCommands(cmds)
	r.sync()

	dlog.Printf("Replica %d is sending PreAccept for (%d.%d, %d):\n", r.Id, r.Id, instance, deps[0])
	r.bcastPreAccept(replica, instance, viewId, ballot, cmds, deps, depViewId)
	//go r.startFastPathClock(&instanceId{r.Id, instance})

	cpcounter += batchSize

	if r.Id == 0 && DO_CHECKPOINTING && cpcounter >= CHECKPOINT_PERIOD {
		cpcounter = 0

		//Propose a checkpoint command to act like a barrier.
		//This allows replicas to discard their dependency hashtables.
		r.crtInstance[r.Id]++
		instance++

		for q := 0; q < r.N; q++ {
			deps[q] = r.crtInstance[q] - 1
		}

		r.InstanceSpace[r.Id][instance] = &Instance{
			cpMarker,
			0,
			latentcopilotproto.PREACCEPTED_EQ,
			deps,
			&LeaderBookkeeping{nil, 0, 0, true, 0, 0, nil, 0, 0, deps, nil, nil, false, false, nil, 0, 1, depViewId, false},
			0,
			0,
			nil, time.Time{}, time.Time{}, false, -1, -1}

		r.latestCPReplica = r.Id
		r.latestCPInstance = instance

		//discard dependency hashtables
		r.clearHashtables()

		r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
		r.sync()

		r.bcastPreAccept(r.Id, instance, viewId, 0, cpMarker, deps, depViewId)
	}
}

func (r *Replica) handlePreAccept(preAccept *latentcopilotproto.PreAccept) {

	// receive preaccept and reply
	r.stat.rpcsRcvd++
	r.stat.rpcsSent++

	// Check if the sender of this PreAccept has the same view
	// If the viewchange is happening for this pilot, or the sender has different view number, REJECT
	if r.views[preAccept.Replica].role != viewchangeproto.ACTIVE || r.views[preAccept.Replica].view.ViewId != preAccept.ViewId {
		// TODO: send reject
		return
	}

	inst := r.InstanceSpace[preAccept.Replica][preAccept.Instance]

	if inst != nil && inst.Status >= latentcopilotproto.ACCEPTED {
		//reordered handling of commit/accept and pre-accept
		if inst.Cmds == nil {
			r.InstanceSpace[preAccept.Replica][preAccept.Instance].Cmds = preAccept.Command
		}
		r.recordCommands(preAccept.Command)
		r.sync()
		return
	}

	if preAccept.Instance >= r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance + 1
	}

	otherLeaderId := 1 - preAccept.Replica
	conflicted, deps, _ := r.checkConflicts(preAccept.Replica, preAccept.Instance, preAccept.Deps, otherLeaderId)

	status := latentcopilotproto.PREACCEPTED_EQ
	if conflicted {
		status = latentcopilotproto.PREACCEPTED
	}

	// Check dep seen
	depSeen := uint8(0)
	if r.views[otherLeaderId].view.ViewId == preAccept.DepViewId {
		if preAccept.Deps[0] < r.crtInstance[otherLeaderId] {
			depSeen = uint8(1)
		} else if r.views[otherLeaderId].role == viewchangeproto.ACTIVE {
			r.crtInstance[otherLeaderId] = preAccept.Deps[0]
			depSeen = uint8(1)
		}
	}
	if inst != nil {
		if preAccept.Ballot < inst.ballot {
			r.replyPreAccept(preAccept.LeaderId,
				&latentcopilotproto.PreAcceptReply{
					preAccept.Replica,
					preAccept.Instance,
					FALSE,
					r.views[preAccept.Replica].view.ViewId,
					inst.ballot,
					inst.Deps,
					r.CommittedUpTo,
					depSeen,
					bool2uint8(r.checkCommandsCommitted(preAccept.Command))})
			return
		} else {
			inst.Cmds = preAccept.Command
			inst.Deps = deps
			inst.ballot = preAccept.Ballot
			inst.Status = status
			inst.acceptBallot = preAccept.Ballot
		}
	} else {
		r.InstanceSpace[preAccept.Replica][preAccept.Instance] = &Instance{
			preAccept.Command,
			preAccept.Ballot,
			status,
			deps,
			nil, 0, 0,
			nil, time.Now(), time.Time{}, false, -1, preAccept.Ballot}
	}

	r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
	r.recordCommands(preAccept.Command)
	r.sync()

	// Update preAcceptedDeps this replica promised for the sender
	r.preAcceptDeps[preAccept.Replica][preAccept.Instance] = deps[0]
	// TODO: send back the seenDep flag

	r.replyPreAccept(preAccept.LeaderId,
		&latentcopilotproto.PreAcceptReply{
			preAccept.Replica,
			preAccept.Instance,
			TRUE,
			r.views[preAccept.Replica].view.ViewId,
			preAccept.Ballot,
			deps,
			r.CommittedUpTo,
			depSeen,
			bool2uint8(r.checkCommandsCommitted(preAccept.Command))})
	if conflicted || preAccept.Replica != preAccept.LeaderId || !isInitialBallot(preAccept.Ballot) {
		dlog.Printf("Replica %d replied to the PreAccept %d.%d with different dep %d; original dep %d; conflict = %v\n", r.Id, preAccept.Replica, preAccept.Instance, deps[0], preAccept.Deps[0], conflicted)
	} else {
		dlog.Printf("Replica %d replied to the PreAccept %d.%d with same original dep %d, conflict = %v\n", r.Id, preAccept.Replica, preAccept.Instance, preAccept.Deps[0], conflicted)
	}

	// step down from being an active pilot if this preaccept has some uncommitted commands
	// and we're outside not-step-down window
	/*if (r.IsLeader1 || r.IsLeader2) && r.IsActive && !r.checkCommandsCommitted(preAccept.Command){
		r.IsActive = false
		r.activeStartTime = time.Now()
	}*/
	dlog.Printf("I've replied to the PreAccept\n")
	//dlog.Printf("Replica %d replied to the PreAccept %d.%d with same original dep %d\n", r.Id, preAccept.Replica, preAccept.Instance, preAccept.Deps[0])
}

func (r *Replica) checkCommandsCommitted(cmds []state.Command) bool {
	for _, cmd := range cmds {
		if !r.committedMap[getKeyForExecMap(cmd.ClientId, cmd.OpId)] {
			return false
		}
	}
	return true
}

func (r *Replica) checkLatency(pilot, inst int32, startTime, endTime time.Time, bound time.Duration, op string) {
	d := endTime.Sub(startTime)
	if d >= bound {
		fmt.Printf("Replica %v: %v (%v.%v) takes %v\n", r.Id, op, pilot, inst, d)
	}
}
func (r *Replica) handlePreAcceptReply(pareply *latentcopilotproto.PreAcceptReply) {

	dlog.Printf("Handling PreAccept reply\n")
	r.stat.rpcsRcvd++
	inst := r.InstanceSpace[pareply.Replica][pareply.Instance]

	if inst == nil || (inst.Status != latentcopilotproto.PREACCEPTED && inst.Status != latentcopilotproto.PREACCEPTED_EQ) {
		// we've moved on, this is a delayed reply
		return
	}

	// validate the view id
	if r.views[pareply.Replica].role != viewchangeproto.ACTIVE || r.views[pareply.Replica].view.ViewId != pareply.ViewId {
		return
	}

	if inst.ballot != pareply.Ballot {
		return
	}

	if pareply.OK == FALSE {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if pareply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = pareply.Ballot
		}
		if inst.lb.nacks >= r.N/2 {
			// TODO
		}
		return
	}

	// update numDepSeens
	if pareply.DepSeen == uint8(1) {
		inst.lb.numDepSeens++
	}
	if inst.lb.numDepSeens > r.N/2 {
		inst.nullDepSafe = true
		inst.depViewId = inst.lb.depViewId
	}

	// update if command has been committed
	if pareply.CmdCommitted == TRUE {
		inst.lb.cmdCommitted = true
	}

	// preAcceptReplies is the number of replies with different dependency
	if pareply.Deps[0] == inst.lb.originalDeps[0] {
		inst.lb.preAcceptOKs++
	} else {
		inst.lb.preAcceptReplies++
	}

	inst.lb.preAcceptReplyDeps = append(inst.lb.preAcceptReplyDeps, pareply.Deps[0])

	if !r.Thrifty && inst.lb.preAcceptOKs+inst.lb.preAcceptReplies < r.N-2 && inst.lb.preAcceptOKs < r.N/2+(r.N/2+1)/2-1 && inst.lb.preAcceptReplies <= r.N/2+1-(r.N/2+1)/2 {
		return
	}

	if r.Thrifty && inst.lb.preAcceptOKs+inst.lb.preAcceptReplies < r.N/2+(r.N/2+1)/2-1 && inst.lb.preAcceptReplies <= r.N/2+1-(r.N/2+1)/2 {
		return
	}

	// Fast path
	// f + (f+1)/2 - 1
	if inst.lb.preAcceptOKs >= r.N/2+(r.N/2+1)/2-1 && isInitialBallot(inst.ballot) {

		inst.Deps[0] = inst.lb.originalDeps[0]
		inst.Status = latentcopilotproto.COMMITTED

		inst.committedTime = time.Now()

		r.updateCommitted(pareply.Replica)
		r.updateCommittedMap(inst.Cmds)
		r.bcastCommit(pareply.Replica, pareply.Instance, inst.Cmds, inst.Deps, inst.nullDepSafe, inst.depViewId)

		// Switch from LATENT to ACTIVE only if the command has not been committed
		//if warmupDone && !r.IsActive && !inst.lb.cmdCommitted {
		if warmupDone && !r.IsActive {
			if !inst.lb.cmdCommitted {
				r.numNewProposals++

				// (Optional) Do fast takeover
				// ensure pilot id/index is either 0 or 1
				if pareply.Replica == int32(0) || pareply.Replica == int32(1) {
					var otherLeaderId = 1 - pareply.Replica
					/*if r.latestTakeover[otherLeaderId] < r.CommittedUpTo[otherLeaderId] {
						r.latestTakeover[otherLeaderId] = r.CommittedUpTo[otherLeaderId]
					}*/
					for i := r.latestTakeover[otherLeaderId] + 1; i <= inst.Deps[0]; i++ {
						depInst := r.InstanceSpace[otherLeaderId][i]
						if depInst != nil && depInst.Status < latentcopilotproto.COMMITTED {
							r.instancesToRecover <- &instanceId{otherLeaderId, i}
							r.latestTakeover[otherLeaderId] = i
						}
					}
				}
			} else {
				r.numNewProposals = 0
			}

			if r.numNewProposals >= MIN_NEW_PROPOSALS_TO_BECOME_ACTIVE {
				r.IsActive = true
				r.activeStartTime = time.Now()
				r.numNewProposals = 0
				fmt.Printf("Replica %v: becomes ACTIVE at %v\n", r.Id, time.Now())
			}
		}

		r.stat.fast++

		dlog.Println("Replica ", r.Id, ": ", inst.lb.preAcceptOKs, inst.lb.preAcceptReplies, ": sending fast commit for instance", pareply.Instance, "with original dep =", inst.Deps[0])

		r.recordInstanceMetadata(inst)
		r.sync() //is this necessary here?

		r.checkLatency(pareply.Replica, pareply.Instance, inst.startTime, inst.committedTime, LATENCY_THRESHOLD, "FAST_COMMIT")
		return
	}

	sort.Sort(int32Array(inst.lb.preAcceptReplyDeps))
	inst.Deps[0] = inst.lb.preAcceptReplyDeps[r.N/2]

	inst.Status = latentcopilotproto.ACCEPTED

	r.stat.slow++

	inst.lb.numDepSeens = 0
	inst.lb.depViewId = -1
	if r.crtInstance[1-pareply.Replica] > inst.Deps[0] {
		inst.lb.numDepSeens = 1
		inst.lb.depViewId = r.views[1-pareply.Replica].view.ViewId
	} else if r.views[1-pareply.Replica].role == viewchangeproto.ACTIVE {
		r.crtInstance[1-pareply.Replica] = inst.Deps[0]
		inst.lb.numDepSeens = 1
		inst.lb.depViewId = r.views[1-pareply.Replica].view.ViewId
	}

	inst.acceptBallot = inst.ballot
	r.bcastAccept(pareply.Replica, pareply.Instance, r.views[pareply.Replica].view.ViewId, inst.ballot, inst.Cmds, inst.Deps, r.views[1-pareply.Replica].view.ViewId)

	// Switch from LATENT to ACTIVE only if the command has not been committed
	if warmupDone && !r.IsActive {
		if !inst.lb.cmdCommitted {
			r.numNewProposals++

			// (Optional) Do fast takeover
			// ensure pilot id/index is either 0 or 1
			if pareply.Replica == int32(0) || pareply.Replica == int32(1) {
				var otherLeaderId = 1 - pareply.Replica
				/*if r.latestTakeover[otherLeaderId] < r.CommittedUpTo[otherLeaderId] {
					r.latestTakeover[otherLeaderId] = r.CommittedUpTo[otherLeaderId]
				}*/
				for i := r.latestTakeover[otherLeaderId] + 1; i <= inst.Deps[0]; i++ {
					depInst := r.InstanceSpace[otherLeaderId][i]
					if depInst != nil && depInst.Status < latentcopilotproto.COMMITTED {
						r.instancesToRecover <- &instanceId{otherLeaderId, i}
						r.latestTakeover[otherLeaderId] = i
					}
				}
			}
		} else {
			r.numNewProposals = 0
		}

		if r.numNewProposals >= MIN_NEW_PROPOSALS_TO_BECOME_ACTIVE {
			r.IsActive = true
			r.activeStartTime = time.Now()
			r.numNewProposals = 0
			fmt.Printf("Replica %v: becomes ACTIVE at %v\n", r.Id, time.Now())
		}
	}

	//TODO: take the slow path if messages are slow to arrive
}

func (r *Replica) runSlowPath(leaderId int32, iid int32) {
	inst := r.InstanceSpace[leaderId][iid]
	if r.views[leaderId].role != viewchangeproto.ACTIVE || inst == nil || (inst.Status != latentcopilotproto.PREACCEPTED && inst.Status != latentcopilotproto.PREACCEPTED_EQ) ||
		(inst.lb.preAcceptOKs+inst.lb.preAcceptReplies) < r.N/2 {
		return
	}

	sort.Sort(int32Array(inst.lb.preAcceptReplyDeps))
	inst.Deps[0] = inst.lb.preAcceptReplyDeps[r.N/2]
	inst.Status = latentcopilotproto.ACCEPTED
	inst.acceptBallot = inst.ballot

	inst.lb.numDepSeens = 0
	inst.lb.depViewId = -1
	if r.crtInstance[1-leaderId] > inst.Deps[0] {
		inst.lb.numDepSeens = 1
		inst.lb.depViewId = r.views[1-leaderId].view.ViewId
	} else if r.views[1-leaderId].role == viewchangeproto.ACTIVE {
		r.crtInstance[1-leaderId] = inst.Deps[0]
		inst.lb.numDepSeens = 1
		inst.lb.depViewId = r.views[1-leaderId].view.ViewId
	}
	dlog.Println("Replica ", r.Id, ": ", inst.lb.preAcceptOKs, inst.lb.preAcceptReplies, inst.lb.preAcceptReplyDeps, ": sending slow accept (b,d) = ", iid, inst.Deps[0])
	r.bcastAccept(leaderId, iid, r.views[leaderId].view.ViewId, inst.ballot, inst.Cmds, inst.Deps, r.views[1-leaderId].view.ViewId)

}

/**********************************************************************

                        PHASE 2

***********************************************************************/

func (r *Replica) handleAccept(accept *latentcopilotproto.Accept) {

	// received accept and reply
	r.stat.rpcsRcvd++
	r.stat.rpcsSent++
	//inst := r.InstanceSpace[accept.LeaderId][accept.Instance]
	inst := r.InstanceSpace[accept.Replica][accept.Instance]

	// Validate view
	// If the viewchange is happening for this pilot, or the sender has different view number, REJECT
	if r.views[accept.Replica].role != viewchangeproto.ACTIVE || r.views[accept.Replica].view.ViewId != accept.ViewId {
		return
	}

	if inst != nil && (inst.Status == latentcopilotproto.COMMITTED || inst.Status == latentcopilotproto.EXECUTED) {
		return
	}

	if accept.Instance >= r.crtInstance[accept.Replica] {
		r.crtInstance[accept.Replica] = accept.Instance + 1
	}

	depSeen := uint8(0)
	if r.views[1-accept.Replica].view.ViewId == accept.DepViewId {
		if accept.Deps[0] < r.crtInstance[1-accept.Replica] {
			depSeen = uint8(1)
		} else if r.views[1-accept.Replica].role == viewchangeproto.ACTIVE {
			r.crtInstance[1-accept.Replica] = accept.Deps[0]
			depSeen = uint8(1)
		}
	}
	if inst != nil {
		if accept.Ballot < inst.ballot {
			r.replyAccept(accept.LeaderId, &latentcopilotproto.AcceptReply{accept.Replica, accept.Instance, FALSE, r.views[accept.Replica].view.ViewId, inst.ballot, depSeen})
			return
		}
		inst.Cmds = accept.Command
		inst.Deps = accept.Deps
		inst.Status = latentcopilotproto.ACCEPTED

		inst.acceptBallot = accept.Ballot
		inst.ballot = accept.Ballot
	} else {
		//r.InstanceSpace[accept.LeaderId][accept.Instance] = &Instance{
		r.InstanceSpace[accept.Replica][accept.Instance] = &Instance{
			accept.Command,
			accept.Ballot,
			latentcopilotproto.ACCEPTED,
			accept.Deps,
			nil, 0, 0, nil, time.Now(), time.Time{}, false, -1, accept.Ballot}
	}

	r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
	r.sync()

	r.replyAccept(accept.LeaderId,
		&latentcopilotproto.AcceptReply{
			accept.Replica,
			accept.Instance,
			TRUE,
			r.views[accept.Replica].view.ViewId,
			accept.Ballot,
			depSeen})
}

func (r *Replica) handleAcceptReply(areply *latentcopilotproto.AcceptReply) {

	r.stat.rpcsRcvd++
	inst := r.InstanceSpace[areply.Replica][areply.Instance]

	if inst.Status != latentcopilotproto.ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	// validate the view id
	if r.views[areply.Replica].role != viewchangeproto.ACTIVE || r.views[areply.Replica].view.ViewId != areply.ViewId {
		return
	}

	if inst.ballot != areply.Ballot {
		return
	}

	if areply.OK == FALSE {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N/2 {
			// TODO
		}
		return
	}

	if areply.DepSeen == uint8(1) {
		inst.lb.numDepSeens++
	}
	if inst.lb.numDepSeens > r.N/2 {
		inst.nullDepSafe = true
		inst.depViewId = inst.lb.depViewId
	}

	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > r.N/2 {
		r.InstanceSpace[areply.Replica][areply.Instance].Status = latentcopilotproto.COMMITTED
		r.updateCommitted(areply.Replica)
		inst.committedTime = time.Now()
		if inst.lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply)
			}
		}

		r.checkLatency(areply.Replica, areply.Instance, inst.startTime, inst.committedTime, LATENCY_THRESHOLD, "COMMIT")
		r.recordInstanceMetadata(inst)
		r.sync() //is this necessary here?

		dlog.Println("Replica ", r.Id, " is sending slow commit (", areply.Replica, areply.Instance, inst.Deps[0], ")")

		r.bcastCommit(areply.Replica, areply.Instance, inst.Cmds, inst.Deps, inst.nullDepSafe, inst.depViewId)
	}
}

/**********************************************************************

                            COMMIT

***********************************************************************/

func (r *Replica) handleCommit(commit *latentcopilotproto.Commit) {

	r.stat.rpcsRcvd++
	inst := r.InstanceSpace[commit.Replica][commit.Instance]
	if inst != nil && (inst.Status == latentcopilotproto.COMMITTED || inst.Status == latentcopilotproto.EXECUTED) {
		return
	}
	dlog.Printf("Replica %d: receive commit for %d(%d.%d, %d)\n", r.Id, commit.LeaderId, commit.Replica, commit.Instance, commit.Deps[0])

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil && len(commit.Command) == 0 {
			// Khiem: we don't re-propose batches in another slot if someone no-op this slot
			/*
				//someone committed a NO-OP, but we have proposals for this instance
				//try in a different instance
				for _, p := range inst.lb.clientProposals {
					r.ProposeChan <- p
				}
			*/
			inst.lb = nil
		}
		//if len(inst.Cmds) == 0 {
		//	inst.Cmds = commit.Command
		//}
		inst.Cmds = commit.Command
		inst.Deps = commit.Deps
		inst.Status = latentcopilotproto.COMMITTED
		inst.committedTime = time.Now()
		if commit.NullDepSafe == TRUE {
			inst.nullDepSafe = true
			inst.depViewId = commit.DepViewId
		}
	} else {
		r.InstanceSpace[commit.Replica][commit.Instance] = &Instance{
			commit.Command,
			0,
			latentcopilotproto.COMMITTED,
			commit.Deps,
			nil,
			0,
			0,
			nil, time.Now(), time.Now(), commit.NullDepSafe == TRUE, commit.DepViewId, -1}
	}
	r.updateCommittedMap(r.InstanceSpace[commit.Replica][commit.Instance].Cmds)
	r.updateCommitted(commit.Replica)

	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)
	dlog.Printf("Replica %d: after committing for %d(%d.%d, %d): new dep = %d\n", r.Id, commit.LeaderId, commit.Replica, commit.Instance, commit.Deps[0], r.InstanceSpace[commit.Replica][commit.Instance].Deps[0])

}

func (r *Replica) handleCommitShort(commit *latentcopilotproto.CommitShort) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {

		if inst.lb != nil && inst.lb.clientProposals != nil {
			/*
				//try command in a different instance
				for _, p := range inst.lb.clientProposals {
					r.ProposeChan <- p
				}
			*/
			//inst.lb = nil
		}
		inst.Deps = commit.Deps
		inst.Status = latentcopilotproto.COMMITTED
		inst.committedTime = time.Now()
		if commit.NullDepSafe == TRUE {
			inst.nullDepSafe = true
			inst.depViewId = commit.DepViewId
		}
	} else {
		r.InstanceSpace[commit.Replica][commit.Instance] = &Instance{
			make([]state.Command, 0),
			0,
			latentcopilotproto.COMMITTED,
			commit.Deps,
			nil, 0, 0, nil, time.Now(), time.Now(), commit.NullDepSafe == TRUE, commit.DepViewId, -1}
	}
	r.updateCommittedMap(r.InstanceSpace[commit.Replica][commit.Instance].Cmds)
	r.updateCommitted(commit.Replica)

	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
}

/**********************************************************************

                      RECOVERY ACTIONS

***********************************************************************/

func (r *Replica) startRecoveryForInstance(replica int32, instance int32) {
	// reschedule recovery later if there is view change
	if r.views[replica].role != viewchangeproto.ACTIVE {
		// should wait for a bit for the view change to complete before re-inserting
		go func() {
			time.Sleep(1 * time.Millisecond)
			r.instancesToRecover <- &instanceId{replica, instance}
		}()
		return
	}

	fmt.Printf("Replica %v: start recovery for (%v.%v)\n", r.Id, replica, instance)
	var nildeps []int32

	if r.InstanceSpace[replica][instance] == nil {
		r.InstanceSpace[replica][instance] = &Instance{make([]state.Command, 0), r.views[replica].defaultBallot, latentcopilotproto.NONE, nildeps, nil, 0, 0, nil, time.Now(), time.Time{}, false, -1, -1}
	}

	// TODO: maybe should check again if the instance changes the status to committed/executed before staring recovery
	inst := r.InstanceSpace[replica][instance]
	if inst.Status == latentcopilotproto.COMMITTED || inst.Status == latentcopilotproto.EXECUTED {
		return
	}
	if inst.lb == nil {
		inst.lb = &LeaderBookkeeping{nil, -1, 0, false, 0, 0, nil, 0, 0, nildeps, nil, nil, true, false, nil, 0, 0, -1, false}
	} else {
		inst.lb = &LeaderBookkeeping{inst.lb.clientProposals, -1, 0, false, 0, 0, nil, 0, 0, nildeps, nil, nil, true, false, nil, 0, 0, -1, false}
	}

	if inst.Status == latentcopilotproto.ACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.Cmds, inst.Status, inst.Deps, 0, false, make([]int32, 0, r.N), 0, false}
		inst.lb.maxRecvBallot = inst.ballot
	} else if inst.Status >= latentcopilotproto.PREACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.Cmds, inst.Status, inst.Deps, 1, r.Id == originalProposerIdFromBallot(inst.ballot), make([]int32, 0, r.N), 0, r.Id == 1-replica}

		if inst.Status == latentcopilotproto.PREACCEPTED_EQ {
			inst.lb.recoveryInst.originalDepCount++
		}
		inst.lb.recoveryInst.prepareReplyDeps = append(inst.lb.recoveryInst.prepareReplyDeps, inst.Deps[0])
	}

	//compute larger ballot
	inst.ballot = r.makeBallotLargerThan(inst.ballot)

	//dlog.Println("Replica ", r.Id, " is starting recovery for ", replica, instance)
	// log.Println("Replica ", r.Id, " is starting recovery for ", replica, instance)
	r.bcastPrepare(replica, instance, r.views[replica].view.ViewId, inst.ballot)
}

func (r *Replica) handlePrepare(prepare *latentcopilotproto.Prepare) {
	inst := r.InstanceSpace[prepare.Replica][prepare.Instance]
	var preply *latentcopilotproto.PrepareReply
	var nildeps []int32
	nilcmd := make([]state.Command, 0)

	// validate view
	if r.views[prepare.Replica].role != viewchangeproto.ACTIVE || r.views[prepare.Replica].view.ViewId != prepare.ViewId {
		preply = &latentcopilotproto.PrepareReply{
			r.Id,
			prepare.Replica,
			prepare.Instance,
			FALSE,
			r.views[prepare.Replica].view.ViewId,
			-1,
			latentcopilotproto.NONE,
			nilcmd,
			nildeps,
			FALSE,
			-1}
	} else if inst == nil {
		if prepare.Ballot < r.views[prepare.Replica].defaultBallot {
			preply = &latentcopilotproto.PrepareReply{
				r.Id,
				prepare.Replica,
				prepare.Instance,
				FALSE,
				r.views[prepare.Replica].view.ViewId,
				r.views[prepare.Replica].defaultBallot,
				latentcopilotproto.NONE,
				nilcmd,
				nildeps,
				FALSE,
				-1}
		} else {
			r.InstanceSpace[prepare.Replica][prepare.Instance] = &Instance{
				nilcmd,
				prepare.Ballot,
				latentcopilotproto.NONE,
				nildeps,
				nil, 0, 0, nil, time.Now(), time.Time{}, false, -1, -1}
			preply = &latentcopilotproto.PrepareReply{
				r.Id,
				prepare.Replica,
				prepare.Instance,
				TRUE,
				r.views[prepare.Replica].view.ViewId,
				-1,
				latentcopilotproto.NONE,
				nilcmd,
				nildeps,
				FALSE,
				-1}
		}
	} else {
		ok := TRUE
		ballot := inst.ballot
		if prepare.Ballot < inst.ballot {
			ok = FALSE
			ballot = inst.ballot
		} else {
			inst.ballot = prepare.Ballot
			ballot = inst.acceptBallot
			// it does not really matter for pre_accepted since the returned ballot is not used.
			/*ballot = inst.ballot
			if inst.Status == latentcopilotproto.ACCEPTED {
				ballot = inst.acceptBallot
			}*/
		}
		preply = &latentcopilotproto.PrepareReply{
			r.Id,
			prepare.Replica,
			prepare.Instance,
			ok,
			r.views[prepare.Replica].view.ViewId,
			ballot,
			inst.Status,
			inst.Cmds,
			inst.Deps,
			bool2uint8(inst.nullDepSafe),
			inst.depViewId}
	}

	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) handlePrepareReply(preply *latentcopilotproto.PrepareReply) {
	inst := r.InstanceSpace[preply.Replica][preply.Instance]

	if inst.lb == nil || !inst.lb.preparing {
		// we've moved on -- these are delayed replies, so just ignore
		// TODO: should replies for non-current ballots be ignored?
		return
	}

	if preply.OK == FALSE {
		// TODO: there is probably another active leader, back off and retry later
		inst.lb.nacks++
		return
	}

	//Got an ACK (preply.OK == TRUE)

	inst.lb.prepareOKs++

	if preply.Status == latentcopilotproto.COMMITTED || preply.Status == latentcopilotproto.EXECUTED {
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			preply.Command,
			inst.ballot,
			latentcopilotproto.COMMITTED,
			preply.Deps,
			nil, 0, 0, nil, time.Now(), time.Now(), preply.NullDepSafe > uint8(0), preply.DepViewId, -1}
		r.bcastCommit(preply.Replica, preply.Instance, inst.Cmds, preply.Deps, inst.nullDepSafe, inst.depViewId)
		//TODO: check if we should send notifications to clients
		return
	}

	if preply.Status == latentcopilotproto.ACCEPTED {
		if inst.lb.recoveryInst == nil || inst.lb.maxRecvBallot < preply.Ballot {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Deps, 0, false, make([]int32, 0, r.N), 0, false}
			inst.lb.maxRecvBallot = preply.Ballot
		}
	}

	// TODO: add another field in recovery for record the number of preaccept_eq_count
	if (preply.Status == latentcopilotproto.PREACCEPTED || preply.Status == latentcopilotproto.PREACCEPTED_EQ) &&
		(inst.lb.recoveryInst == nil || inst.lb.recoveryInst.status < latentcopilotproto.ACCEPTED) {
		if inst.lb.recoveryInst == nil {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Deps, 0, false, make([]int32, 0, r.N), 0, false}
		}
		if (inst.lb.recoveryInst.cmds == nil || len(inst.lb.recoveryInst.cmds) == 0) && (preply.Command != nil && len(preply.Command) > 0) {
			inst.lb.recoveryInst.cmds = preply.Command
		}
		inst.lb.recoveryInst.preAcceptCount++

		if preply.Status == latentcopilotproto.PREACCEPTED_EQ {
			inst.lb.recoveryInst.originalDepCount++
			inst.lb.recoveryInst.deps = preply.Deps
		}

		if preply.Status == latentcopilotproto.PREACCEPTED || preply.Status == latentcopilotproto.PREACCEPTED_EQ {
			inst.lb.recoveryInst.prepareReplyDeps = append(inst.lb.recoveryInst.prepareReplyDeps, preply.Deps[0])
		}
		/*if preply.AcceptorId == preply.Replica {
			inst.lb.recoveryInst.leaderResponded = true
		} else if preply.AcceptorId == 1-preply.Replica {
			inst.lb.recoveryInst.otherLeaderResponded = true
		}*/

		if preply.AcceptorId == originalProposerIdFromBallot(preply.Ballot) {
			inst.lb.recoveryInst.leaderResponded = true
		}
	}

	//if inst.lb.prepareOKs < r.N/2 {
	//TODO: change back to r.N/2 later
	if inst.lb.prepareOKs < r.N-2 {
		return
	}

	//Received Prepare replies from a majority

	ir := inst.lb.recoveryInst

	//fastQS := r.N/2 + (r.N/2+1)/2
	slowQS := r.N/2 + 1

	depViewId := r.views[1-preply.Replica].view.ViewId
	if ir != nil {
		//Case 1 (2.3): at least one replica has accepted this instance
		if ir.status == latentcopilotproto.ACCEPTED ||
			(!ir.leaderResponded && ir.originalDepCount >= slowQS-1) || (ir.leaderResponded && ir.originalDepCount >= slowQS) {
			inst.Cmds = ir.cmds
			inst.Deps = ir.deps
			inst.Status = latentcopilotproto.ACCEPTED
			inst.lb.preparing = false
			inst.acceptBallot = inst.ballot
			if inst.Deps[0] < r.crtInstance[1-preply.Replica] {
				inst.lb.numDepSeens = 1
				inst.lb.depViewId = depViewId
			}
			r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, inst.Deps, depViewId)
			//dlog.Println(r.Id, "...in here 1...")
		} else {
			deps := []int32{-1}
			inst.lb.preparing = false
			r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
				make([]state.Command, 0),
				inst.ballot,
				latentcopilotproto.ACCEPTED,
				deps,
				inst.lb, 0, 0, nil, time.Now(), time.Time{}, false, -1, inst.ballot}
			r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, deps, depViewId)
		}
	} else { // (2.1 as in pseudocode)
		dlog.Println(r.Id, "...in here 4...")
		//try to finalize instance by proposing NO-OP

		// we set to empty since we implicitly enforce the ordering between batches from the same leader
		deps := []int32{-1}
		inst.lb.preparing = false
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			make([]state.Command, 0),
			inst.ballot,
			latentcopilotproto.ACCEPTED,
			deps,
			inst.lb, 0, 0, nil, time.Now(), time.Time{}, false, -1, inst.ballot}
		r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, deps, depViewId)
	}
}

func (r *Replica) handlePrepareReplyWIP(preply *latentcopilotproto.PrepareReply) {
	inst := r.InstanceSpace[preply.Replica][preply.Instance]

	if inst.lb == nil || !inst.lb.preparing {
		// we've moved on -- these are delayed replies, so just ignore
		// TODO: should replies for non-current ballots be ignored?
		return
	}

	if preply.OK == FALSE {
		// TODO: there is probably another active leader, back off and retry later
		inst.lb.nacks++
		return
	}

	inst.lb.prepareOKs++

	if preply.Status == latentcopilotproto.COMMITTED || preply.Status == latentcopilotproto.EXECUTED {
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			preply.Command,
			inst.ballot,
			latentcopilotproto.COMMITTED,
			preply.Deps,
			nil, 0, 0, nil, time.Now(), time.Now(), preply.NullDepSafe > uint8(0), preply.DepViewId, -1}
		r.bcastCommit(preply.Replica, preply.Instance, inst.Cmds, preply.Deps, inst.nullDepSafe, inst.depViewId)
		//TODO: check if we should send notifications to clients
		return
	}

	if preply.Status == latentcopilotproto.ACCEPTED {
		if inst.lb.recoveryInst == nil || inst.lb.maxRecvBallot < preply.Ballot {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Deps, 0, false, make([]int32, 0, r.N), 0, false}
			inst.lb.maxRecvBallot = preply.Ballot
		}
	}

	// TODO: add another field in recovery for record the number of preaccept_eq_count
	if (preply.Status == latentcopilotproto.PREACCEPTED || preply.Status == latentcopilotproto.PREACCEPTED_EQ) &&
		(inst.lb.recoveryInst == nil || inst.lb.recoveryInst.status < latentcopilotproto.ACCEPTED) {
		if inst.lb.recoveryInst == nil {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Deps, 0, false, make([]int32, 0, r.N), 0, false}
		}
		if (inst.lb.recoveryInst.cmds == nil || len(inst.lb.recoveryInst.cmds) == 0) && (preply.Command != nil && len(preply.Command) > 0) {
			inst.lb.recoveryInst.cmds = preply.Command
		}
		inst.lb.recoveryInst.preAcceptCount++

		if preply.Status == latentcopilotproto.PREACCEPTED_EQ {
			inst.lb.recoveryInst.originalDepCount++
			inst.lb.recoveryInst.deps = preply.Deps
		}

		if preply.Status == latentcopilotproto.PREACCEPTED || preply.Status == latentcopilotproto.PREACCEPTED_EQ {
			inst.lb.recoveryInst.prepareReplyDeps = append(inst.lb.recoveryInst.prepareReplyDeps, preply.Deps[0])
		}

		if preply.AcceptorId == originalProposerIdFromBallot(preply.Ballot) {
			inst.lb.recoveryInst.leaderResponded = true
		}
	}

	if inst.lb.prepareOKs < r.N/2 {
		return
	}

	ir := inst.lb.recoveryInst

	// fastQS := r.N/2 + (r.N/2+1)/2
	slowQS := r.N/2 + 1

	depViewId := r.views[1-preply.Replica].view.ViewId
	if ir != nil {
		//Case 1 (2.3): at least one replica has accepted this instance
		if ir.status == latentcopilotproto.ACCEPTED ||
			(!ir.leaderResponded && ir.originalDepCount >= slowQS-1) || (ir.leaderResponded && ir.originalDepCount >= slowQS) {
			inst.Cmds = ir.cmds
			inst.Deps = ir.deps
			inst.Status = latentcopilotproto.ACCEPTED
			inst.lb.preparing = false
			inst.acceptBallot = inst.ballot
			if inst.Deps[0] < r.crtInstance[1-preply.Replica] {
				inst.lb.numDepSeens = 1
				inst.lb.depViewId = depViewId
			}
			r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, inst.Deps, depViewId)
			//dlog.Println(r.Id, "...in here 1...")
		} else if ir.leaderResponded || ir.originalDepCount < (r.N/2+1)/2 {
			// ir.originalDepCount must be < slowQS
			// since we don't hit the committed/accepted cases above
			// that means the original propose could not commit the instance
			// and would not be able to since it already replied to this prepare
			// it's okay to noop
			deps := []int32{-1}
			inst.lb.preparing = false
			r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
				make([]state.Command, 0),
				inst.ballot,
				latentcopilotproto.ACCEPTED,
				deps,
				inst.lb, 0, 0, nil, time.Now(), time.Time{}, false, -1, inst.ballot}
			r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, deps, depViewId)
		} else { /*ir.leaderResponded == false AND f > |FA_OKs| >= (f+1)/2*/
			if len(inst.lb.preAcceptReplyDeps) <= r.N/2 {
				fmt.Printf("Replica %v: recovery for (%v.%v). Not enough deps. Need to send PreAccept to more. Deps = %v\n", r.Id, preply.Replica, preply.Instance, inst.lb.preAcceptReplyDeps)
				return
			}
			sort.Sort(int32Array(inst.lb.preAcceptReplyDeps))
			var unresolvedDeps []int32
			// Check the conflicting dependencies
			for i := inst.lb.recoveryInst.deps[0] + 1; i <= inst.lb.preAcceptReplyDeps[len(inst.lb.preAcceptReplyDeps)-1]; i++ {
				if r.InstanceSpace[1-preply.Replica][i] != nil && r.InstanceSpace[1-preply.Replica][i].Status >= latentcopilotproto.COMMITTED &&
					r.InstanceSpace[1-preply.Replica][i].Cmds != nil && len(r.InstanceSpace[1-preply.Replica][i].Cmds) > 0 &&
					r.InstanceSpace[1-preply.Replica][i].Deps[0] < preply.Instance {
					fmt.Printf("Replica %v: found (%v.%v) commit with dep %v. Commit (%v.%v) with no-op\n", r.Id, 1-preply.Replica, i, r.InstanceSpace[1-preply.Replica][i].Deps[0], preply.Replica, preply.Instance)
					// concurrent entry committed with dep before this instance -> this instance could not succeed on fast path
					// commit no-op
					deps := []int32{-1}
					inst.lb.preparing = false
					r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
						make([]state.Command, 0),
						inst.ballot,
						latentcopilotproto.ACCEPTED,
						deps,
						inst.lb, 0, 0, nil, time.Now(), time.Time{}, false, -1, inst.ballot}
					r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, deps, depViewId)
					return
				}
				if r.InstanceSpace[1-preply.Replica][i] != nil && r.InstanceSpace[1-preply.Replica][i].Status >= latentcopilotproto.COMMITTED {
					continue
				}
				unresolvedDeps = append(unresolvedDeps, i)
			}
			if len(unresolvedDeps) == 0 {
				fmt.Printf("Replica %v: concurrent entries have dep >= %v. Commit (%v.%v) with original dep %v\n", r.Id, inst.Deps[0], preply.Replica, preply.Instance, inst.Deps[0])
				// all concurrent deps have dep >= preply.Instance. safe to commit with original dep
				inst.Cmds = ir.cmds
				inst.Deps = ir.deps
				inst.Status = latentcopilotproto.ACCEPTED
				inst.lb.preparing = false
				inst.acceptBallot = inst.ballot
				r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, inst.Deps, depViewId)
				return
			}
			// let the concurrent entries be resolved and try recover later
			go func() {
				fmt.Printf("Replica %v: Unresolved entries %v when recovering (%v.%v)\n", r.Id, unresolvedDeps, preply.Replica, preply.Instance)
				time.Sleep(1 * time.Millisecond)
				r.instancesToRecover <- &instanceId{preply.Replica, preply.Instance}
				return
			}()
		}
	} else {
		dlog.Println(r.Id, "...in here 4...")
		//try to finalize instance by proposing NO-OP
		// we set to empty since we implicitly enforce the ordering between batches from the same leader
		deps := []int32{-1}
		inst.lb.preparing = false
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			make([]state.Command, 0),
			inst.ballot,
			latentcopilotproto.ACCEPTED,
			deps,
			inst.lb, 0, 0, nil, time.Now(), time.Time{}, false, -1, inst.ballot}
		r.bcastAccept(preply.Replica, preply.Instance, r.views[preply.Replica].view.ViewId, inst.ballot, inst.Cmds, deps, depViewId)
	}
}

func (r *Replica) handleTryPreAccept(tpa *latentcopilotproto.TryPreAccept) {
	inst := r.InstanceSpace[tpa.Replica][tpa.Instance]
	if inst != nil && inst.ballot > tpa.Ballot {
		// ballot number too small
		r.replyTryPreAccept(tpa.LeaderId, &latentcopilotproto.TryPreAcceptReply{
			r.Id,
			tpa.Replica,
			tpa.Instance,
			FALSE,
			inst.ballot,
			tpa.Replica,
			tpa.Instance,
			inst.Status})

		return
	}

	if conflict, confRep, confInst := r.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, -1, tpa.Deps); conflict {
		// there is a conflict, can't pre-accept
		r.replyTryPreAccept(tpa.LeaderId, &latentcopilotproto.TryPreAcceptReply{
			r.Id,
			tpa.Replica,
			tpa.Instance,
			FALSE,
			inst.ballot,
			confRep,
			confInst,
			r.InstanceSpace[confRep][confInst].Status})
	} else {
		// can pre-accept
		if tpa.Instance >= r.crtInstance[tpa.Replica] {
			r.crtInstance[tpa.Replica] = tpa.Instance + 1
		}
		if inst != nil {
			inst.Cmds = tpa.Command
			inst.Deps = tpa.Deps
			inst.Status = latentcopilotproto.PREACCEPTED
			inst.ballot = tpa.Ballot
		} else {
			r.InstanceSpace[tpa.Replica][tpa.Instance] = &Instance{
				tpa.Command,
				tpa.Ballot,
				latentcopilotproto.PREACCEPTED,
				tpa.Deps,
				nil, 0, 0,
				nil, time.Now(), time.Time{}, false, -1, -1}
		}
		r.replyTryPreAccept(tpa.LeaderId, &latentcopilotproto.TryPreAcceptReply{r.Id, tpa.Replica, tpa.Instance, TRUE, inst.ballot, 0, 0, 0})
	}
}

func (r *Replica) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	/*inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.Cmds) > 0 {
		if inst.Status >= latentcopilotproto.ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}

		if tpa.Deps != nil && len(tpa.Deps) > 0 && equal(inst.Deps, tpa.Deps) {
			// already PRE-ACCEPTED, no point looking for conflicts again
			return false, replica, instance
		}

	}*/

	q := 1 - replica
	//for i := r.ExecedUpTo[q]; i < r.crtInstance[q]; i++ {
	for i := r.crtInstance[q] - 1; i >= r.ExecedUpTo[q]; i-- {
		// implicit dependency between batches from same leader
		// so this instance will be after deps[0] which is after i <= deps[0]
		if i <= deps[0] {
			break
		}

		inst := r.InstanceSpace[q][i]
		if inst == nil || inst.Cmds == nil || len(inst.Cmds) == 0 {
			continue
		}
		if inst.Deps[0] >= instance {
			// instance q.i depends on instance replica.instance, it is not a conflict
			continue
		}
		return true, q, i
	}

	return false, -1, -1
}

func (r *Replica) handleTryPreAcceptReply(tpar *latentcopilotproto.TryPreAcceptReply) {
	inst := r.InstanceSpace[tpar.Replica][tpar.Instance]
	if inst == nil || inst.lb == nil || !inst.lb.tryingToPreAccept || inst.lb.recoveryInst == nil {
		return
	}

	ir := inst.lb.recoveryInst

	if tpar.OK == TRUE {
		inst.lb.preAcceptOKs++
		inst.lb.tpaOKs++
		if inst.lb.preAcceptOKs >= r.N/2 {
			//it's safe to start Accept phase
			inst.Cmds = ir.cmds
			inst.Deps = ir.deps
			inst.Status = latentcopilotproto.ACCEPTED
			inst.lb.tryingToPreAccept = false
			inst.lb.acceptOKs = 0
			inst.acceptBallot = inst.ballot
			r.bcastAccept(tpar.Replica, tpar.Instance, r.views[tpar.Replica].view.ViewId, inst.ballot, inst.Cmds, inst.Deps, r.views[1-tpa.Replica].view.ViewId)
			return
		}
	} else {
		inst.lb.nacks++
		if tpar.Ballot > inst.ballot {
			//TODO: retry with higher ballot
			return
		}
		inst.lb.tpaOKs++
		if tpar.ConflictReplica == tpar.Replica && tpar.ConflictInstance == tpar.Instance {
			//TODO: re-run prepare
			inst.lb.tryingToPreAccept = false
			return
		}
		inst.lb.possibleQuorum[tpar.AcceptorId] = false
		inst.lb.possibleQuorum[tpar.ConflictReplica] = false
		notInQuorum := 0
		for q := 0; q < r.N; q++ {
			if !inst.lb.possibleQuorum[tpar.AcceptorId] {
				notInQuorum++
			}
		}
		if tpar.ConflictStatus >= latentcopilotproto.COMMITTED || notInQuorum > r.N/2 {
			//abandon recovery, restart from phase 1
			inst.lb.tryingToPreAccept = false
			r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
		}
		if notInQuorum == r.N/2 {
			//this is to prevent defer cycles
			if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
				if inst.lb.possibleQuorum[dq] {
					//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
					//abandon recovery, restart from phase 1
					inst.lb.tryingToPreAccept = false
					r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
				}
			}
		}
		if inst.lb.tpaOKs >= r.N/2 {
			//defer recovery and update deferred information
			updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
			inst.lb.tryingToPreAccept = false
		}
	}
}

/**********************************************************************

                      VIEW CHANGE PROTOCOL

***********************************************************************/
func (r *Replica) startViewChange(pilotId int32) {

	currViewState := r.views[pilotId]
	newViewId := r.makeViewIdLargerThan(currViewState.proposedViewId)

	currViewState.role = viewchangeproto.MANAGER
	currViewState.status = viewchangeproto.PREPARING

	currViewState.proposedViewId = newViewId

	currViewState.vmb = &ViewManagerBookkeeping{0, r.CommittedUpTo[pilotId], r.crtInstance[pilotId], nil, 0, 0, 0, 0, 0}
	r.bcastViewChange(r.Id, pilotId, *r.views[pilotId].view, newViewId)
}

func (r *Replica) handleViewChange(viewchange *viewchangeproto.ViewChange) {

	pilot := viewchange.PilotId

	if pilot >= NUM_LEADERS {
		return
	}
	currViewState := r.views[viewchange.PilotId]
	viewManagerId := viewchange.ViewManagerId

	// Case 1: old view of viewchange request is old. Reject
	// reply with ViewChangeReject
	if viewchange.OldView.ViewId < currViewState.view.ViewId {

		var viewchangeReject *viewchangeproto.ViewChangeReply
		proposedViewId := viewchange.NewViewId
		if proposedViewId < currViewState.proposedViewId {
			proposedViewId = currViewState.proposedViewId
		}
		viewchangeReject = &viewchangeproto.ViewChangeReply{
			PilotId:       pilot,
			ViewId:        viewchange.NewViewId,
			MaxSeenViewId: proposedViewId,
			OK:            FALSE,
			OldView:       *currViewState.view, // send back updated old view
		}
		r.replyViewChange(viewManagerId, viewchangeReject)
		return
	}

	// Case 2: proposed view id is smaller than some known proposed view id
	// reply with ViewChangeReject
	if viewchange.NewViewId < currViewState.proposedViewId {
		updatedOldView := viewchange.OldView
		if updatedOldView.ViewId < currViewState.view.ViewId {
			updatedOldView = *currViewState.view
		} else if updatedOldView.ViewId > currViewState.view.ViewId {
			// update view to at least old view in viewchange
			// TODO: Should we do more than just update the view?
			// To do state transfer to bring log up to date
			// before updating new view info.
			copyView(updatedOldView, currViewState.view)
		}

		var viewchangeReject *viewchangeproto.ViewChangeReply
		viewchangeReject = &viewchangeproto.ViewChangeReply{
			PilotId:       pilot,
			ViewId:        viewchange.NewViewId,
			MaxSeenViewId: currViewState.proposedViewId,
			OK:            FALSE,
			OldView:       updatedOldView,
		}
		r.replyViewChange(viewManagerId, viewchangeReject)
		return
	}

	// update highest proposed view id seen so far
	currViewState.proposedViewId = viewchange.NewViewId

	// Case 3: same old view but this replica has accepted some view
	if viewchange.OldView.ViewId == currViewState.view.ViewId && currViewState.acceptedView != nil {
		currViewState.role = viewchangeproto.UNDERLING

		var viewchangeOK *viewchangeproto.ViewChangeReply
		viewchangeOK = &viewchangeproto.ViewChangeReply{
			PilotId:                 pilot,
			ViewId:                  viewchange.NewViewId,
			OK:                      TRUE,
			MaxSeenViewId:           currViewState.proposedViewId,
			LatestCommittedInstance: max(currViewState.acceptedCommittedInstance, r.CommittedUpTo[pilot]), // r.CommittedUpTo[pilot],
			LatestInstance:          currViewState.acceptedCurrentInstance,                                //max(currViewState.acceptedCurrentInstance, r.crtInstance[pilot]), // TODO: think. max(acceptedCrt, r.crt)?
			AcceptedView:            *currViewState.acceptedView,
		}
		currViewState.proposedViewId = viewchange.NewViewId
		r.replyViewChange(viewManagerId, viewchangeOK)
		return
	}

	// Case 4: either this replica has even outdated old view,
	// or same old view but has not accepted any view
	currViewState.role = viewchangeproto.UNDERLING

	// update this replica view if it is outdated
	// TODO: consider update log here to bring it to most updated
	if currViewState.view.ViewId < viewchange.OldView.ViewId {
		copyView(viewchange.OldView, currViewState.view)
	}
	// Set any accepted view to null
	currViewState.acceptedView = nil

	var viewchangeOK *viewchangeproto.ViewChangeReply
	viewchangeOK = &viewchangeproto.ViewChangeReply{
		PilotId:                 pilot,
		ViewId:                  viewchange.NewViewId,
		OK:                      TRUE,
		MaxSeenViewId:           currViewState.proposedViewId,
		LatestCommittedInstance: r.CommittedUpTo[pilot],
		LatestInstance:          r.crtInstance[pilot],
		AcceptedView:            viewchangeproto.View{},
	}
	currViewState.proposedViewId = viewchange.NewViewId
	r.replyViewChange(viewManagerId, viewchangeOK)
}

func (r *Replica) handleViewChangeReply(vcReply *viewchangeproto.ViewChangeReply) {
	// Ignore if this reply is for older views
	if vcReply.PilotId >= NUM_LEADERS || r.views[vcReply.PilotId].view.ViewId > vcReply.ViewId || r.views[vcReply.PilotId].role != viewchangeproto.MANAGER || r.views[vcReply.PilotId].status != viewchangeproto.PREPARING {
		return
	}

	currViewState := r.views[vcReply.PilotId]

	if vcReply.OK == TRUE {
		// increase +1 for vc bookeeping
		currViewState.vmb.viewchangeOKs++

		if vcReply.AcceptedView != (viewchangeproto.View{}) {
			if currViewState.vmb.acceptedView == nil || *currViewState.vmb.acceptedView == (viewchangeproto.View{}) || vcReply.AcceptedView.ViewId > currViewState.vmb.acceptedView.ViewId {
				if currViewState.vmb.acceptedView == nil {
					currViewState.vmb.acceptedView = &viewchangeproto.View{}
				}
				copyView(vcReply.AcceptedView, currViewState.vmb.acceptedView)
				currViewState.vmb.acceptedCommittedInstance = vcReply.LatestCommittedInstance
				currViewState.vmb.acceptedCurrentInstance = vcReply.LatestInstance
			}
		}

		// latestCommitted will help find maxCommitted, and serve as a criteria to select a pilot
		// maxCommitted helps a replicas to walk back the log to bring itself up to date
		if currViewState.vmb.maxCommittedInstance < vcReply.LatestCommittedInstance {
			currViewState.vmb.maxCommittedInstance = vcReply.LatestCommittedInstance
		}

		if currViewState.vmb.maxCurrentInstance < vcReply.LatestInstance {
			currViewState.vmb.maxCurrentInstance = vcReply.LatestInstance
		}

		// TODO: for general view change where the number of replicas may change
		// the majority size should be updated (check majority of old view and majority
		// of new view). Here we assume the view change only changes the leader
		if currViewState.vmb.viewchangeOKs >= r.N/2 {
			currViewState.status = viewchangeproto.PREPARED
			currViewState.vmb.nacks = 0

			if currViewState.acceptedView == nil {
				currViewState.acceptedView = &viewchangeproto.View{}
			}

			// Case 1: If no replicas accepted any view
			if currViewState.vmb.acceptedView == nil || *currViewState.vmb.acceptedView == (viewchangeproto.View{}) {
				// Construct new view
				newView := r.constructNewView(vcReply.ViewId, vcReply.PilotId, r.Id)
				copyView(*newView, currViewState.acceptedView)
				currViewState.acceptedCurrentInstance = currViewState.vmb.maxCurrentInstance
				currViewState.acceptedCommittedInstance = currViewState.vmb.maxCommittedInstance

				r.bcastAcceptView(r.Id, vcReply.PilotId, currViewState.acceptedCommittedInstance, currViewState.acceptedCurrentInstance, *newView)
			} else {
				// Use the accepted view associated with the highest viewId received
				copyView(*currViewState.vmb.acceptedView, currViewState.acceptedView)

				/* note: we use acceptedCurrentInstance so that this new view starts at the same
				   instance as the accepted view which may have been committed.
				*/
				// currViewState.acceptedCurrentInstance = currViewState.vmb.maxCurrentInstance
				currViewState.acceptedCurrentInstance = currViewState.vmb.acceptedCurrentInstance
				currViewState.acceptedCommittedInstance = currViewState.vmb.maxCommittedInstance

				// update ViewId of new view to be this view manager's proposed view id
				currViewState.acceptedView.ViewId = vcReply.ViewId

				r.bcastAcceptView(r.Id, vcReply.PilotId, currViewState.acceptedCommittedInstance, currViewState.acceptedCurrentInstance, *currViewState.vmb.acceptedView)
			}
		}
	} else {
		currViewState.vmb.nacks++
		if vcReply.MaxSeenViewId > currViewState.proposedViewId {
			currViewState.proposedViewId = vcReply.MaxSeenViewId
		}
		// Steps down from being a view manager
		// (Note: another option would be setting currViewState.role = viewchangeproto.UNDERLING)
		currViewState.role = viewchangeproto.ACTIVE
		currViewState.vmb = nil
		currViewState.acceptedView = nil
		//currViewState.role = viewchangeproto.UNDERLING
	}

}

func (r *Replica) handleAcceptView(acceptView *viewchangeproto.AcceptView) {
	pilot := acceptView.PilotId

	if pilot >= NUM_LEADERS {
		return
	}
	currViewState := r.views[acceptView.PilotId]
	viewManagerId := acceptView.ViewManagerId

	var avreply *viewchangeproto.AcceptViewReply
	// this replica already moved on with later view
	// or accept some view propser with higher id
	if currViewState.view.ViewId > acceptView.NewView.ViewId || currViewState.proposedViewId > acceptView.NewView.ViewId {
		avreply = &viewchangeproto.AcceptViewReply{
			PilotId: acceptView.PilotId,
			ViewId:  acceptView.NewView.ViewId,
			OK:      FALSE,
		}
		r.replyAcceptView(viewManagerId, avreply)
		return
	}

	currViewState.proposedViewId = acceptView.NewView.ViewId
	// TODO: update local view and state transfer

	avreply = &viewchangeproto.AcceptViewReply{
		PilotId: acceptView.PilotId,
		ViewId:  acceptView.NewView.ViewId,
		OK:      TRUE,
	}
	r.replyAcceptView(viewManagerId, avreply)
}

func (r *Replica) handleAcceptViewReply(acceptViewReply *viewchangeproto.AcceptViewReply) {
	// Ignore if this reply is for older views
	if acceptViewReply.PilotId >= NUM_LEADERS ||
		r.views[acceptViewReply.PilotId].view.ViewId > acceptViewReply.ViewId ||
		r.views[acceptViewReply.PilotId].role != viewchangeproto.MANAGER ||
		(r.views[acceptViewReply.PilotId].status != viewchangeproto.PREPARED && r.views[acceptViewReply.PilotId].status != viewchangeproto.ACCEPTED) {
		return
	}

	currViewState := r.views[acceptViewReply.PilotId]

	if acceptViewReply.OK == TRUE {
		currViewState.vmb.acceptViewOKs++
		if currViewState.vmb.acceptViewOKs+1 > r.N>>1 {

			// reset all instances >= currViewState.acceptedCurrentInstance
			// this can happen if this replica receives a proposal from failed pilot
			// and this proposal reached a minority of replicas before this pilot failed.
			// TODO: this is not safe yet since the instance may be concurrently accessed by the execution!!!
			for i := currViewState.acceptedCurrentInstance; i < r.crtInstance[acceptViewReply.PilotId]; i++ {
				r.InstanceSpace[acceptViewReply.PilotId] = nil
			}
			// update current instance
			r.crtInstance[acceptViewReply.PilotId] = currViewState.acceptedCurrentInstance

			var nildeps []int32
			for i := r.CommittedUpTo[acceptViewReply.PilotId] + 1; i < r.crtInstance[acceptViewReply.PilotId]; i++ {
				if r.InstanceSpace[acceptViewReply.PilotId][i] == nil {
					// TODO: set ballot to default ballot number of old view
					r.InstanceSpace[acceptViewReply.PilotId][i] = &Instance{nil, currViewState.defaultBallot, latentcopilotproto.NONE, nildeps, nil, 0, 0, nil, time.Now(), time.Time{}, false, -1, -1}
				}
			}

			// TODO: set default ballot for instances

			// TODO: fill in steps
			// update local view to new view: curr_view = new_view
			// update next entry to start proposing: nextEntry = max_current+1
			// update prepare
			// keep track which starting entry this view starts
			r.views[acceptViewReply.PilotId].startInstance = currViewState.acceptedCurrentInstance
			copyView(*currViewState.acceptedView, currViewState.view)
			r.bcastStartView(acceptViewReply.PilotId, *currViewState.acceptedView, currViewState.acceptedCurrentInstance)
			currViewState.role = viewchangeproto.ACTIVE
			currViewState.status = viewchangeproto.INITIATED

			currViewState.defaultBallot = makeBallot(0, currViewState.view.ReplicaId, currViewState.view.ReplicaId)

			// clear some bookkeeping states
			currViewState.vmb = nil
			currViewState.acceptedView = nil

			// update isLeaderX
			if currViewState.view.ReplicaId == r.Id {
				if acceptViewReply.PilotId == 0 {
					fmt.Printf("Replica %v becomes new pilot 0: %v %v %v %v\n", r.Id, r.CommittedUpTo[0], r.crtInstance[0], currViewState.acceptedCommittedInstance, currViewState.acceptedCurrentInstance)
					r.IsLeader1 = true
					if PINGPONG && PINGPONG_TIMEOUT_ENABLED {
						go r.pingpongTimeoutClock()
					}
				} else if acceptViewReply.PilotId == 1 {
					fmt.Printf("Replica %v becomes new pilot 1: %v %v %v %v\n", r.Id, r.CommittedUpTo[1], r.crtInstance[1], currViewState.acceptedCommittedInstance, currViewState.acceptedCurrentInstance)
					r.IsLeader2 = true
					if PINGPONG && PINGPONG_TIMEOUT_ENABLED {
						go r.pingpongTimeoutClock()
					}
				}
				// fill gaps. not very necessary since execution will eventually do the fast takeover
				// uncomment this following code if we want to fill gaps without waiting for fast takeover timeout
				go func() {
					time.Sleep(500 * time.Microsecond)
					for i := r.CommittedUpTo[acceptViewReply.PilotId] + 1; i < r.crtInstance[acceptViewReply.PilotId]; i++ {
						r.instancesToRecover <- &instanceId{acceptViewReply.PilotId, i}
					}
					return
				}()
			} else {
				if acceptViewReply.PilotId == 0 {
					r.IsLeader1 = false
				}
				if acceptViewReply.PilotId == 1 {
					r.IsLeader2 = false
				}
			}
		}

	} else {
		currViewState.vmb.nacks++
		// TODO: should i update my highest proposed viewid???

		// TODO: we assume the number of replicas remain the same for now
		// i.e., no adding or removing replicas
		if currViewState.vmb.nacks >= r.N>>1 {
			currViewState.role = viewchangeproto.UNDERLING
		}

	}
}

func (r *Replica) handleStartView(initView *viewchangeproto.InitView) {
	// TODO: add more checks to make sure this commit is not outdated
	// in case there are concurrent view changes
	if initView.PilotId >= NUM_LEADERS || r.views[initView.PilotId].view.ViewId >= initView.NewView.ViewId {
		return
	}

	// TODO: this is not safe yet since the instance may be concurrently accessed by the execution
	for i := initView.LatestInstance; i < r.crtInstance[initView.PilotId]; i++ {
		r.InstanceSpace[initView.PilotId] = nil
	}
	// update current instance
	r.crtInstance[initView.PilotId] = initView.LatestInstance

	var nildeps []int32
	for i := r.CommittedUpTo[initView.PilotId] + 1; i < r.crtInstance[initView.PilotId]; i++ {
		if r.InstanceSpace[initView.PilotId][i] == nil {
			// TODO: set ballot to default ballot of old view
			r.InstanceSpace[initView.PilotId][i] = &Instance{nil, r.views[initView.PilotId].defaultBallot, latentcopilotproto.NONE, nildeps, nil, 0, 0, nil, time.Now(), time.Time{}, false, -1, -1}
		}
	}
	// TODO: set new default ballot for this pilot new view

	// keep track which starting entry this view starts
	r.views[initView.PilotId].startInstance = initView.LatestInstance
	// Update current view and state of view
	currViewState := r.views[initView.PilotId]
	copyView(initView.NewView, currViewState.view)
	currViewState.role = viewchangeproto.ACTIVE
	currViewState.status = viewchangeproto.INITIATED

	currViewState.defaultBallot = makeBallot(0, initView.NewView.ReplicaId, initView.NewView.ReplicaId)

	currViewState.acceptedView = nil

	// update isLeaderX
	if initView.NewView.ReplicaId == r.Id {
		if initView.PilotId == 0 {
			r.IsLeader1 = true
		}
		if initView.PilotId == 1 {
			r.IsLeader2 = true
		}
		// fill gaps. not very necessary since execution will eventually do the fast takeover
		// uncomment this following code if we want to fill gaps without waiting for fast takeover timeout
		go func() {
			time.Sleep(500 * time.Microsecond)
			for i := r.CommittedUpTo[initView.PilotId] + 1; i < r.crtInstance[initView.PilotId]; i++ {
				r.instancesToRecover <- &instanceId{initView.PilotId, i}
			}
			return
		}()
	} else {
		if initView.PilotId == 0 {
			if r.IsLeader1 && initView.NewView.ReplicaId != r.Id {
				fmt.Printf("Replica %v steps down from being pilot 0\n", r.Id)
			}
			r.IsLeader1 = false
		}
		if initView.PilotId == 1 {
			if r.IsLeader2 && initView.NewView.ReplicaId != r.Id {
				fmt.Printf("Replica %v steps down from being pilot 1\n", r.Id)
			}
			r.IsLeader2 = false
		}
	}
}

var vc viewchangeproto.ViewChange

func (r *Replica) bcastViewChange(viewManagerId int32, pilotId int32, oldView viewchangeproto.View, newViewId int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("ViewChange bcast failed:", err)
		}
	}()
	vc.ViewManagerId = viewManagerId
	vc.PilotId = pilotId
	vc.OldView = oldView
	vc.NewViewId = newViewId
	args := &vc

	n := r.N - 1
	if r.Thrifty {
		n = r.N/2 + 1
	}

	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			dlog.Println("Not enough replicas alive!")
			break
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.viewchangeRPC, args)
		sent++
	}
}

var av viewchangeproto.AcceptView

func (r *Replica) bcastAcceptView(viewManagerId int32, pilotId int32, latestCommittedInstance int32, LatestInstance int32, newView viewchangeproto.View) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("AcceptView bcast failed:", err)
		}
	}()

	av.ViewManagerId = viewManagerId
	av.PilotId = pilotId
	av.LatestCommittedInstance = latestCommittedInstance
	av.LatestInstance = LatestInstance
	copyView(newView, &(av.NewView))
	args := &av

	n := r.N - 1
	if r.Thrifty {
		n = r.N/2 + 1
	}

	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			dlog.Println("Not enough replicas alive!")
			break
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.acceptViewRPC, args)
		sent++
	}
}

var sv viewchangeproto.InitView

func (r *Replica) bcastStartView(pilotId int32, newView viewchangeproto.View, latestInstance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("StartView bcast failed:", err)
		}
	}()

	sv.PilotId = pilotId
	sv.NewView = newView
	sv.LatestInstance = latestInstance
	args := &sv

	n := r.N - 1
	if r.Thrifty {
		n = r.N/2 + 1
	}

	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			dlog.Println("Not enough replicas alive!")
			break
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.initViewRPC, args)
		sent++
	}
}

func (r *Replica) constructNewView(viewId int32, pilotId int32, replicaId int32) *viewchangeproto.View {
	return &viewchangeproto.View{viewId, pilotId, replicaId}
}

/**********************************************************************

                      VIEW CHANGE: INTER-REPLICA COMMUNICATION

***********************************************************************/
/*func (r *Replica) replyViewChangeOK(replicaId int32, reply *viewchangeproto.ViewChangeOK) {
	r.SendMsg(replicaId, r.viewchangeOKRPC, reply)
}

func (r *Replica) replyViewChangeReject(replicaId int32, reply *viewchangeproto.ViewChangeReject) {
	r.SendMsg(replicaId, r.viewchangeRejectRPC, reply)
}*/

func (r *Replica) replyViewChange(replicaId int32, reply *viewchangeproto.ViewChangeReply) {
	r.SendMsg(replicaId, r.viewchangeReplyRPC, reply)
}

func (r *Replica) replyAcceptView(replicaId int32, reply *viewchangeproto.AcceptViewReply) {
	r.SendMsg(replicaId, r.acceptViewReplyRPC, reply)
}

// Handle GetView request from client
func (r *Replica) handleGetViewFromClient(getView *genericsmr.GetView) {
	var getViewReply *genericsmrproto.GetViewReply
	if r.views[getView.PilotId].role == viewchangeproto.ACTIVE {
		getViewReply = &genericsmrproto.GetViewReply{
			OK:        TRUE,
			ViewId:    r.views[getView.PilotId].view.ViewId,
			PilotId:   r.views[getView.PilotId].view.PilotId,
			ReplicaId: r.views[getView.PilotId].view.ReplicaId,
		}
	} else {
		getViewReply = &genericsmrproto.GetViewReply{
			OK:        FALSE,
			ViewId:    0,
			PilotId:   getView.PilotId,
			ReplicaId: 0,
		}
	}
	r.ReplyGetView(getViewReply, getView.Reply)
}

//helper functions and structures to prevent defer cycles while recovering

var deferMap map[uint64]uint64 = make(map[uint64]uint64)

func updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}

/* Helper interface for sorting int32 */
type int32Array []int32

func (arr int32Array) Len() int {
	return len(arr)
}

func (arr int32Array) Less(i, j int) bool {
	return arr[i] < arr[j]
}

func (arr int32Array) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

/* Helper functions for view */
func copyView(fromView viewchangeproto.View, toView *viewchangeproto.View) {
	toView.ViewId = fromView.ViewId
	toView.PilotId = fromView.PilotId
	toView.ReplicaId = fromView.ReplicaId

}

/* Other helper functions */
func max(a, b int32) int32 {
	if a > b {
		return a
	}

	return b
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}

	return b
}

func bool2uint8(a bool) uint8 {
	if a {
		return uint8(1)
	}
	return uint8(0)
}
