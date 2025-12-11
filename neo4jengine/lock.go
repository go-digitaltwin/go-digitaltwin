package neo4jengine

import (
	"sync"
)

// We are not fully acquainted with Neo4j isolation levels, and have observed
// that two concurrent transactions might share data. Therefore, we need to
// prevent any graph modifications while we're reading it.
//
// This issue caused incoherent data where attributes would flip between valid
// values and empty states within seconds. Investigation revealed that the diff
// calculation between snapshots (running every ~15 seconds) could observe
// inconsistent graph states when concurrent writes modified node relationships.
// The root cause was Neo4j's isolation not being sufficient for our read
// consistency requirements during snapshot capture.
//
// To enforce this type of locking, we are introducing the graphWRMutex, an
// adaptation of sync.RWMutex to suit the specific locking requirements within
// the context of Neo4j graph operations, in which multiple concurrent write
// transactions are permissible, but read operations must be exclusive. The zero
// value for a graphWRMutex is an unlocked mutex.
//
// The guarantees provided by sync.RWMutex regarding the Go memory model,
// especially the "synchronises before" relationship, apply here as well. Thus,
// the n'th call to WUnlock precedes the m'th call to Lock. Likewise, for each
// call to Lock, there exists a call to WUnlock that precedes it, ensuring proper
// synchronisation.
type graphWRMutex sync.RWMutex

// WLock locks wr for writing. It should not be used for recursive write locking;
// a blocked Lock call excludes new writers from acquiring the lock.
func (wr *graphWRMutex) WLock() {
	(*sync.RWMutex)(wr).RLock()
}

// WUnlock undoes a single WLock call; it does not affect other simultaneous
// writers. It is a run-time error if wr is not locked for writing on entry to
// WUnlock.
func (wr *graphWRMutex) WUnlock() {
	(*sync.RWMutex)(wr).RUnlock()
}

// Lock locks wr for reading. If the lock is already locked for writing or
// reading, Lock blocks until the lock is available.
func (wr *graphWRMutex) Lock() {
	(*sync.RWMutex)(wr).Lock()
}

// Unlock unlocking wr for reading. It is a run-time error if wr is not locked
// for reading on entry to Unlock.
func (wr *graphWRMutex) Unlock() {
	(*sync.RWMutex)(wr).Unlock()
}
