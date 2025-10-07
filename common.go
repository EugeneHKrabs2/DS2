package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrNotReady    = "ErrNotReady"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	OperationID int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OperationID int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type UpdateBackupValues struct {
	KeyValue map[string]string
	Seen     map[int64]SeenOperation
}

type BackupReply struct {
	Err Err
}

type ForwardedPutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	OperationID int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ForwardedPutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}
