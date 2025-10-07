package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type SeenOperation struct {
	Done bool
	Prev string
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.

	clientLock sync.Mutex
	viewLock   sync.Mutex
	recentView viewservice.View
	KeyValue   map[string]string
	Seen       map[int64]SeenOperation
	Backedup   bool
}

func (pb *PBServer) ForwardPut(args *ForwardedPutArgs, reply *ForwardedPutReply) error {
	pb.viewLock.Lock()
	backupCheck := pb.recentView.Backup == pb.me
	installed := pb.Backedup
	pb.viewLock.Unlock()

	if backupCheck == false {
		reply.Err = ErrWrongServer
		return nil
	}

	if installed == false {
		reply.Err = ErrNotReady
		return nil
	}

	pb.clientLock.Lock()
	if value, ok := pb.Seen[args.OperationID]; ok && value.Done {
		reply.Err = OK
		reply.PreviousValue = value.Prev
		pb.clientLock.Unlock()
		return nil
	}

	old := pb.KeyValue[args.Key]
	if args.DoHash {
		new := hash(old + args.Value)
		pb.KeyValue[args.Key] = strconv.Itoa(int(new))
	} else {
		pb.KeyValue[args.Key] = args.Value
	}

	pb.Seen[args.OperationID] = SeenOperation{Done: true, Prev: old}
	pb.clientLock.Unlock()
	reply.Err = OK
	reply.PreviousValue = old
	return nil
}

func (pb *PBServer) InstallDB(args *UpdateBackupValues, reply *BackupReply) error {
	pb.viewLock.Lock()
	backupCheck := pb.recentView.Backup == pb.me
	pb.viewLock.Unlock()

	if backupCheck == false {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.clientLock.Lock()
	pb.KeyValue = make(map[string]string, len(args.KeyValue))
	for key, value := range args.KeyValue {
		pb.KeyValue[key] = value
	}

	pb.Seen = make(map[int64]SeenOperation, len(args.Seen))
	for key, value := range args.Seen {
		pb.Seen[key] = value
	}

	pb.clientLock.Unlock()
	pb.Backedup = true
	reply.Err = OK
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.viewLock.Lock()
	view := pb.recentView
	pb.viewLock.Unlock()

	if view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	if view.Backup != "" && pb.Backedup == false {
		reply.Err = ErrNotReady
		return nil
	}

	pb.clientLock.Lock()
	defer pb.clientLock.Unlock()

	if value, ok := pb.Seen[args.OperationID]; ok && value.Done {
		reply.Err = OK
		reply.PreviousValue = value.Prev
		return nil
	}

	old := pb.KeyValue[args.Key]

	if args.DoHash {
		new := hash(old + args.Value)
		pb.KeyValue[args.Key] = strconv.Itoa(int(new))
	} else {
		pb.KeyValue[args.Key] = args.Value
	}
	pb.Seen[args.OperationID] = SeenOperation{Done: true, Prev: old}

	if view.Backup != "" {
		forwardArgs := ForwardedPutArgs{
			Key:         args.Key,
			Value:       args.Value,
			DoHash:      args.DoHash,
			OperationID: args.OperationID,
		}
		var forwardReply ForwardedPutReply
		ok := false

		for i := 0; i < 5; i++ {
			ok = call(view.Backup, "PBServer.ForwardPut", &forwardArgs, &forwardReply)
			if ok == true && forwardReply.Err == OK {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if ok == false || forwardReply.Err != OK {
			pb.viewLock.Lock()
			pb.Backedup = false
			pb.viewLock.Unlock()
			reply.Err = ErrNotReady
			return nil
		}
	}

	reply.Err = OK
	reply.PreviousValue = old
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.viewLock.Lock()
	primaryCheck := pb.recentView.Primary == pb.me
	pb.viewLock.Unlock()

	if primaryCheck == false {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.clientLock.Lock()
	value, ok := pb.KeyValue[args.Key]
	pb.clientLock.Unlock()
	if ok == false {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = value
	}
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.viewLock.Lock()
	oldView := pb.recentView
	view, _ := pb.vs.Ping(oldView.Viewnum)
	pb.recentView = view
	pb.viewLock.Unlock()

	if pb.me == view.Primary && oldView.Primary != pb.me {
		pb.viewLock.Lock()
		pb.Backedup = false
		pb.viewLock.Unlock()
	}
	if pb.me == view.Backup && oldView.Backup != pb.me {
		pb.viewLock.Lock()
		pb.Backedup = false
		pb.viewLock.Unlock()
	}

	if pb.me == view.Primary && view.Backup != "" && view.Backup != oldView.Backup {
		go func() {
			pb.clientLock.Lock()
			copyKeyValue := make(map[string]string, len(pb.KeyValue))
			for key, value := range pb.KeyValue {
				copyKeyValue[key] = value
			}
			copySeen := make(map[int64]SeenOperation, len(pb.Seen))
			for key, value := range pb.Seen {
				copySeen[key] = value
			}
			pb.clientLock.Unlock()

			args := &UpdateBackupValues{KeyValue: copyKeyValue, Seen: copySeen}
			var backupReply BackupReply

			for {
				ok := call(view.Backup, "PBServer.InstallDB", args, &backupReply)
				if ok && backupReply.Err == OK {
					pb.viewLock.Lock()
					pb.Backedup = true
					pb.viewLock.Unlock()
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.KeyValue = make(map[string]string)
	pb.Seen = make(map[int64]SeenOperation)
	pb.Backedup = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
