package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

type Instance struct {
	Np   int         //highest proposal number
	Na   int         // highest accepted proposal
	Va   interface{} // value of highest accepted proposal
	Done bool        //whether consensus has been reached
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*Instance
	done      map[int]int
}

type ProposeArgs struct {
	N   int
	Seq int
}

type ProposeReply struct {
	Na          int
	Va          interface{}
	Ok          bool
	HighestDone int
}

type AcceptArgs struct {
	N   int
	Seq int
	V   interface{}
}

type AcceptReply struct {
	Ok          bool
	Na          int
	Va          interface{}
	HighestDone int
}

type DecideArgs struct {
	N   int
	Seq int
	V   interface{}
}

type DecideReply struct {
	Ok          bool
	HighestDone int
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Propose(args *ProposeArgs, reply *ProposeReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, ok := px.instances[args.Seq]
	if ok == false {
		instance = &Instance{Np: -1, Na: -1, Va: nil, Done: false}
		px.instances[args.Seq] = instance
	}

	if px.instances[args.Seq].Done { //already got one, send back what it should be
		reply.Na = px.instances[args.Seq].Na
		reply.Va = px.instances[args.Seq].Va
		reply.Ok = true
		reply.HighestDone = px.done[px.me]
		fmt.Printf("I, %v,Already done in propose, Na: %v Va: %v, Seq: %v\n", px.me, px.instances[args.Seq].Na, px.instances[args.Seq].Va, args.Seq)
		return nil
	}
	currentN := px.instances[args.Seq].Np

	if args.N < currentN {
		reply.Ok = false
		reply.Na = px.instances[args.Seq].Na
		reply.Va = px.instances[args.Seq].Va
		reply.HighestDone = px.done[px.me]
		return nil
	}

	if args.N >= currentN {
		px.instances[args.Seq].Np = args.N
		reply.Na = px.instances[args.Seq].Na
		reply.Va = px.instances[args.Seq].Va
		reply.Ok = true
		reply.HighestDone = px.done[px.me]
		return nil
	}

	return nil

}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, ok := px.instances[args.Seq]
	if ok == false {
		instance = &Instance{Np: -1, Na: -1, Va: nil, Done: false}
		px.instances[args.Seq] = instance
	}

	if px.instances[args.Seq].Done {
		if args.V == px.instances[args.Seq].Va {
			reply.Ok = true
			reply.Na = px.instances[args.Seq].Na
			reply.Va = px.instances[args.Seq].Va
		} else {
			reply.Ok = false
			reply.Na = px.instances[args.Seq].Na
			reply.Va = px.instances[args.Seq].Va
		}
		reply.HighestDone = px.done[px.me]
		px.instances[args.Seq].Done = true
		fmt.Printf("Tried: V: %v already done giving Na: %v and Va: %v on seq: %v\n", args.V, px.instances[args.Seq].Na, px.instances[args.Seq].Va, args.Seq)
		return nil
	}

	if args.N < px.instances[args.Seq].Np {
		reply.Ok = false
		reply.Na = px.instances[args.Seq].Na
		reply.Va = px.instances[args.Seq].Va
		reply.HighestDone = px.done[px.me]
		return nil
	}
	if args.N >= px.instances[args.Seq].Np {
		px.instances[args.Seq].Np = args.N
		px.instances[args.Seq].Va = args.V
		px.instances[args.Seq].Na = args.N
		reply.Ok = true
		reply.HighestDone = px.done[px.me]
		reply.Na = args.N
		reply.Va = args.V
		fmt.Printf("Accepting new values from %v\n", px.me)
	} else {
		reply.Ok = false
		reply.HighestDone = px.done[px.me]
		reply.Na = args.N
		reply.Va = args.V
	}
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, ok := px.instances[args.Seq]
	if ok == false {
		instance = &Instance{Np: -1, Na: -1, Va: nil, Done: false}
		px.instances[args.Seq] = instance
	}

	if args.N >= px.instances[args.Seq].Na {
		px.instances[args.Seq].Done = true
		px.instances[args.Seq].Na = args.N
		px.instances[args.Seq].Np = args.N
		px.instances[args.Seq].Va = args.V
		reply.HighestDone = px.done[px.me]
		reply.Ok = true
		fmt.Printf("I %v, have decided seq: %v, with Va: %v\n", px.me, args.Seq, px.instances[args.Seq].Va)
		return nil
	}
	return nil
}

func (px *Paxos) Proposer(seq int, v interface{}) error {
	// prepare
	attempt := 0
	for {
		n := attempt*len(px.peers) + px.me
		attempt++
		fmt.Printf("I, %v, proposing seq: %v, V: %v, N: %v\n", px.me, seq, v, n)
		majorityCount := 0
		maxNa := -1
		Va := v
		proposeArgs := ProposeArgs{N: n, Seq: seq}

		for i, peer := range px.peers {
			var proposeReply ProposeReply
			//fmt.Printf("I'm %v and I am proposing %v, on seq: %v to %v, with n: %v\n", px.me, Va, seq, i, n)
			if call(peer, "Paxos.Propose", &proposeArgs, &proposeReply) {
				if proposeReply.Ok {
					majorityCount++
				}
				if proposeReply.Na > maxNa {
					fmt.Printf("Replacing my N:%v with Na: %v on seq: %v\n", maxNa, proposeReply.Na, seq)
					maxNa = proposeReply.Na
					if proposeReply.Va != nil {
						Va = proposeReply.Va
						fmt.Printf("I, %v,Replacing my V:%v with Va: %v on seq: %v\n", px.me, v, Va, seq)
					}
				}
				px.mu.Lock()
				if px.done[i] < proposeReply.HighestDone {
					px.done[i] = proposeReply.HighestDone
				}
				px.mu.Unlock()
			}
		}

		fmt.Printf("I, %v, Propose Majority for seq: %v, N: %v,Max Na: %v, majority: %v\n", px.me, seq, n, maxNa, majorityCount)
		fmt.Printf("Va: %v\n", Va)
		print("\n")
		if majorityCount < len(px.peers)/2+1 {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		// accept

		acceptCount := 0
		acceptArgs := AcceptArgs{N: n, Seq: seq, V: Va}

		for i, peer := range px.peers {
			var acceptReply AcceptReply
			if call(peer, "Paxos.Accept", &acceptArgs, &acceptReply) {
				if acceptReply.Ok {
					acceptCount++
				}
				px.mu.Lock()
				if px.done[i] < acceptReply.HighestDone {
					px.done[i] = acceptReply.HighestDone
				}
				px.mu.Unlock()
			}
		}
		fmt.Printf("I, %v,Accept Majority for seq: %v, N: %v,Max Na: %v, majority: %v\n", px.me, seq, n, maxNa, majorityCount)
		fmt.Printf("Va: %v\n", Va)
		if acceptCount < len(px.peers)/2+1 {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		//decide
		decideArgs := DecideArgs{N: n, Seq: seq, V: Va}
		for i, peer := range px.peers {
			var decideReply DecideReply
			//fmt.Printf("Trying to decide\n")
			if peer == px.peers[px.me] {
				px.mu.Lock()

				instance, ok := px.instances[seq]
				if ok == false {
					instance = &Instance{Np: -1, Na: -1, Va: nil, Done: false}
					px.instances[seq] = instance
				}

				px.instances[seq].Np = n
				px.instances[seq].Va = Va
				px.instances[seq].Na = n
				px.instances[seq].Done = true
				fmt.Printf("I, %v, am deciding myself on seq: %v, with Va: %v\n", px.me, seq, Va)

				px.mu.Unlock()
			} else {
				call(peer, "Paxos.Decide", &decideArgs, &decideReply)
				px.mu.Lock()
				if px.done[i] < decideReply.HighestDone {
					px.done[i] = decideReply.HighestDone
				}
				px.mu.Unlock()
			}
		}
		return nil

	}

}

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.mu.Lock()

	if px.instances[seq] == nil {
		px.instances[seq] = &Instance{Np: -1, Na: -1}
	}
	px.mu.Unlock()
	go px.Proposer(seq, v)
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.done[px.me] < seq {
		px.done[px.me] = seq
	}
	return
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	maxSeq := -1

	for seq := range px.instances {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.done[px.me]
	for _, val := range px.done {
		if val < min {
			min = val
		}
	}
	//fmt.Printf("I, %v, think min is %v\n", px.me, min)
	for seq := range px.instances {
		if seq < min+1 {
			delete(px.instances, seq)
		}
	}
	return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, ok := px.instances[seq]
	if !ok || instance == nil {
		return false, nil
	}
	if px.instances[seq].Done {
		return true, px.instances[seq].Va
	}
	return false, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*Instance)
	px.done = make(map[int]int)

	for i := range peers {
		px.done[i] = -1
	}
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
