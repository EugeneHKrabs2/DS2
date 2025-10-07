package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu          sync.Mutex
	l           net.Listener
	dead        bool
	me          string
	view        View
	primary_ack bool

	servers   map[string]time.Time
	restarted map[string]bool
	// Your declarations here.
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.servers[args.Me] = time.Now()

	if args.Viewnum == 0 {
		if args.Me == vs.view.Primary || args.Me == vs.view.Backup {
			vs.restarted[args.Me] = true
		} else {
			delete(vs.restarted, args.Me)
		}
	} else {
		delete(vs.restarted, args.Me)
	}

	if args.Viewnum == vs.view.Viewnum && args.Me == vs.view.Primary {
		vs.primary_ack = true
	}

	reply.View = vs.view

	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// lock
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.view

	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	primarycheck := vs.restarted[vs.view.Primary]
	backupcheck := vs.restarted[vs.view.Backup]

	if vs.view.Viewnum == 0 {
		for key, value := range vs.servers {
			if key != "" && time.Since(value) <= DeadPings*PingInterval {
				vs.view = View{1, key, ""}
				vs.primary_ack = false
				delete(vs.restarted, key)
				return
			}
		}
		return
	}

	oldPrimary := vs.view.Primary
	oldBackup := vs.view.Backup

	if ((vs.view.Primary != "" && time.Since(vs.servers[vs.view.Primary]) >= DeadPings*PingInterval) || primarycheck) && vs.primary_ack {
		primary_replacement := vs.view.Backup
		backup_replacement := ""

		if primary_replacement != "" && time.Since(vs.servers[primary_replacement]) <= DeadPings*PingInterval && !vs.restarted[primary_replacement] {
			for key, _ := range vs.servers {
				if key != primary_replacement && key != vs.view.Primary && time.Since(vs.servers[key]) <= DeadPings*PingInterval {
					backup_replacement = key
					break
				}
			}
		}

		if primary_replacement != "" && (primary_replacement != vs.view.Primary || backup_replacement != vs.view.Backup) && time.Since(vs.servers[primary_replacement]) <= DeadPings*PingInterval && !vs.restarted[primary_replacement] {
			vs.view = View{vs.view.Viewnum + 1, primary_replacement, backup_replacement}
			vs.primary_ack = false
			if primarycheck {
				delete(vs.restarted, oldPrimary)
			}
			if primary_replacement != "" {
				delete(vs.restarted, primary_replacement)
			}
			if backup_replacement != "" {
				delete(vs.restarted, backup_replacement)
			}
			return
		}
	}

	if ((vs.view.Backup != "" && time.Since(vs.servers[vs.view.Backup]) >= DeadPings*PingInterval) || backupcheck) && vs.primary_ack {
		for key, value := range vs.servers {
			if key != vs.view.Backup && key != vs.view.Primary && time.Since(value) <= DeadPings*PingInterval {
				vs.view = View{vs.view.Viewnum + 1, vs.view.Primary, key}
				vs.primary_ack = false
				if backupcheck {
					delete(vs.restarted, oldBackup)
				}

				delete(vs.restarted, key)
				return
			}
		}
		vs.view = View{vs.view.Viewnum + 1, vs.view.Primary, ""}
		vs.primary_ack = false
		if backupcheck {
			delete(vs.restarted, oldBackup)
		}
		return
	}

	if vs.view.Backup == "" && vs.primary_ack {
		for key, value := range vs.servers {
			if key != vs.view.Primary && time.Since(value) <= DeadPings*PingInterval {
				vs.view = View{vs.view.Viewnum + 1, vs.view.Primary, key}
				vs.primary_ack = false
				delete(vs.restarted, key)
				return
			}
		}
	}

}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.servers = make(map[string]time.Time)
	vs.restarted = make(map[string]bool)
	vs.view = View{0, "", ""}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
