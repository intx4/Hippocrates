package gossip

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/project"

	"go.dedis.ch/cs438/hw3/gossip/types"
)

// VectorClock keeps track of all received and sent rumors as well as the next rumor ID expected
// from a peer
type VectorClock struct {
	nextID   map[string]uint32         // peer name -> next message id
	messages map[string][]RumorMessage // peer name -> list of rumors
	sync.Mutex
}

func (vc *VectorClock) init(origin string) {
	// locked from other functions !!
	if _, present := vc.nextID[origin]; !present {
		vc.messages[origin] = make([]RumorMessage, 0)
		vc.nextID[origin] = 1
	}
}

func (vc *VectorClock) generateNewRumor(myName, text string) GossipPacket {
	vc.Lock()
	defer vc.Unlock()
	vc.init(myName)
	id := vc.nextID[myName]
	msg := RumorMessage{
		Text:   text,
		Origin: myName,
		ID:     id,
	}

	pkt := GossipPacket{
		Rumor: &msg,
	}

	vc.messages[myName] = append(vc.messages[myName], msg)
	vc.nextID[myName]++
	return pkt
}

//HW3
func (vc *VectorClock) generateNewRumor_Paxos(myName, text string, paxos *types.ExtraMessage, key []byte) GossipPacket {
	vc.Lock()
	defer vc.Unlock()
	vc.init(myName)
	id := vc.nextID[myName]
	msg := RumorMessage{
		Text:   text,
		Origin: myName,
		ID:     id,
		Extra:  paxos.Encrypt(key), //Encrypt
	}

	pkt := GossipPacket{
		Rumor: &msg,
	}

	vc.messages[myName] = append(vc.messages[myName], msg)
	vc.nextID[myName]++
	return pkt
}

//Project
func (vc *VectorClock) generateNewRumor_Dkg(myName, text string, pkMessage *project.PkMessage, resp *project.DealResponse) GossipPacket {
	vc.Lock()
	defer vc.Unlock()
	vc.init(myName)
	id := vc.nextID[myName]
	msg := RumorMessage{
		Text:         text,
		Origin:       myName,
		ID:           id,
		PkMessage:    pkMessage,
		DealResponse: resp,
	}

	pkt := GossipPacket{
		Rumor: &msg,
	}

	vc.messages[myName] = append(vc.messages[myName], msg)
	vc.nextID[myName]++
	return pkt
}
func (vc *VectorClock) newRumor(r RumorMessage) bool {
	origin := r.Origin
	id := r.ID
	vc.Lock()
	defer vc.Unlock()
	vc.init(origin)

	// if in order, store the new rumor and increment counter
	if id == vc.nextID[origin] {
		vc.messages[origin] = append(vc.messages[origin], r)
		vc.nextID[origin]++
		return true
	}
	return false
}

func (vc *VectorClock) getStatus() []PeerStatus {
	vc.Lock()
	defer vc.Unlock()
	status := make([]PeerStatus, 0)
	for peer, nextID := range vc.nextID {
		// don't include self in status if we haven't sent any messages yet (see hw1 moodle discussion)
		// own id is only added with init() in GenerateNewRumor
		// NewRumor and CompareStatus can't init() with own id unless we did it already
		/*if peer == me && nextID == 1 {
			continue
		}*/
		status = append(status, PeerStatus{peer, nextID})
	}
	return status
}

func (vc *VectorClock) compareStatus(g *Gossiper, other []PeerStatus) (bool, bool, []RumorMessage) {
	vc.Lock()
	defer vc.Unlock()

	sendStatus := false
	rumorsToSend := make([]RumorMessage, 0)

	// set to keep track of all peer identifiers (union of both sets)
	all := make(map[string]bool)
	for s := range vc.nextID {
		all[s] = true
	}
	// put others in a map for easier lookup
	them := make(map[string]uint32)
	for _, s := range other {
		them[s.Identifier] = s.NextID
		all[s.Identifier] = true
	}

	// iterate over all peers and check if they are in both `vc.nextId` and `them`
	for peer := range all {
		thisID, inThis := vc.nextID[peer]
		thatID, inThat := them[peer]

		if !inThis && inThat { // we don't have peer, send status to get updated
			sendStatus = true
			//vc.init(peer) // lazy init, we wait until they actually send us a rumor -> NewRumor calls init()
		} else if inThis && !inThat { // they don't have peer, send them all messages from given peer
			for _, rumor := range vc.messages[peer] {
				rumorsToSend = append(rumorsToSend, rumor)
			}
		} else { // both have peer, decide based on id values
			if thatID > thisID {
				sendStatus = true
			} else if thisID > thatID {
				for _, rumor := range vc.messages[peer][thatID-1:] {
					rumorsToSend = append(rumorsToSend, rumor)
				}
			}
		}
	}

	upToDate := !sendStatus && len(rumorsToSend) == 0

	return upToDate, sendStatus, rumorsToSend
}

func statusString(status []PeerStatus) string {
	s := ""
	for _, ps := range status {
		s += fmt.Sprintf("peer %s nextID %d ", ps.Identifier, ps.NextID)
	}
	return strings.TrimSpace(s)
}

/////////////

const timeout = 10 * time.Second

// PendingMessage groups a rumor and the node it was sent to
type PendingMessage struct {
	receiver string
	message  RumorMessage
}

// PendingMessages is used to store pending messages until they are acked by the node they were sent to
type PendingMessages struct {
	pending map[PendingMessage]*time.Timer
	sync.Mutex
}

func (pm *PendingMessages) cleanUp() {
	pm.Lock()
	for _, t := range pm.pending {
		t.Stop()
	}
	// don't unlock
}

func (pm *PendingMessages) remove(m PendingMessage) {
	// !!! called from other functions
	if t, ok := pm.pending[m]; ok {
		t.Stop()
	}
	delete(pm.pending, m)
}

func (pm *PendingMessages) add(g *Gossiper, m PendingMessage) {
	pm.Lock()
	defer pm.Unlock()
	//log.Lvlf1("MONGERING with %s\n", m.receiver)
	timer := time.NewTimer(timeout)
	pm.pending[m] = timer
	go func() {
		<-timer.C
		pm.Lock()
		pm.remove(m) // TODO: check if need to lock dissss
		pm.Unlock()
		dst, ok := g.sendToRandomExcept(GossipPacket{Rumor: &m.message}, m.receiver)
		if ok {
			pm.add(g, PendingMessage{dst, m.message})
		}
	}()
}

func (pm *PendingMessages) isPending(m PendingMessage) bool {
	pm.Lock()
	defer pm.Unlock()
	_, ok := pm.pending[m]
	return ok
}

func (pm *PendingMessages) ackPendingForPeer(status *StatusPacket, peer string) (RumorMessage, bool) {
	pm.Lock()
	defer pm.Unlock()
	rumors := make([]RumorMessage, 0)
	for k := range pm.pending {
		if k.receiver == peer {
			rumors = append(rumors, k.message)
		}
	}
	ackedRumor, isAck := ackPending(status, rumors)
	if isAck {
		pm.remove(PendingMessage{peer, ackedRumor})
	}
	return ackedRumor, isAck
}

func ackPending(status *StatusPacket, rumors []RumorMessage) (RumorMessage, bool) {
	statusMap := make(map[string]uint32)
	for _, peerStatus := range status.Want {
		statusMap[peerStatus.Identifier] = peerStatus.NextID
	}
	for _, rumor := range rumors {
		if nextID, ok := statusMap[rumor.Origin]; ok {
			if nextID == (rumor.ID + 1) {
				return rumor, true
			}
		}
	}
	return RumorMessage{}, false
}
