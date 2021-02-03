// ========== CS-438 HW2 Skeleton ===========
// *** Implement here the handler for simple message processing ***

package gossip

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"math/rand"
	"net"
	"path/filepath"
	"strings"
	//"go.dedis.ch/onet/v3/log"
)

// Exec is the function that the gossiper uses to execute the handler for a SimpleMessage
// processSimple processes a SimpleMessage as such:
// - add message's relay address to the known peers
// - update the relay field
func (msg *SimpleMessage) Exec(g *Gossiper, addr *net.UDPAddr) error {
	//log.Lvlf1("SIMPLE MESSAGE origin %s from %s contents %s\n", msg.OriginPeerName, msg.RelayPeerAddr, msg.Contents)

	err := g.AddAddresses(msg.RelayPeerAddr)
	msg.RelayPeerAddr = g.addrStr
	g.nodes.print()
	return err
}

// Exec is the function that the gossiper uses to execute the handler for a RumorMessage
func (msg *RumorMessage) Exec(g *Gossiper, addr *net.UDPAddr) error {
	//Modified for HW3 to handle Paxos Logic
	var err error
	src := addr.String()
	fromMe := (src == g.conn.LocalAddr().String())
	//update new peer
	if !fromMe {
		err = g.AddAddresses(src)
	}
	//log.Lvlf1("RUMOR origin %s from %s ID %d contents %s\n", msg.Origin, src, msg.ID, msg.Text)
	isPaxos := msg.isPaxos()
	//process paxos
	if isPaxos {
		//HANDLER FOR PAXOS
		if msg.Extra.PaxosPrepare != nil { //PREPARE
			g.PromiseValidation(src, msg.Extra.PaxosPrepare)
		} else if msg.Extra.PaxosPromise != nil { //PROMISE
			g.ListenerManager(msg.Extra)
		} else if msg.Extra.PaxosPropose != nil { //PROPOSE
			g.AcceptValidation(msg.Extra.PaxosPropose)
		} else if msg.Extra.PaxosAccept != nil { //ACCEPT
			g.ListenerManager(msg.Extra)
		} else if msg.Extra.TLC != nil { //TLC
			g.ListenerManager(msg.Extra)
		}
	}
	// update routing table and vector clock
	isRouteRumor := (msg.isRouteRumor() && !isPaxos) //in Paxos packets we have also txt = ""
	if !fromMe {
		g.routes.newRumor(msg, src)
	}
	isNewRumor := g.vc.newRumor(*msg)
	//g.nodes.print()
	if isNewRumor {
		pkt := GossipPacket{Rumor: msg}
		if g.callback != nil && !isRouteRumor { // TODO: check
			g.callback(msg.Origin, pkt)
		}
		dst, ok := g.sendToRandomExcept(pkt, src) // don't forward to the same peer we received from
		if ok {
			g.pm.add(g, PendingMessage{dst, *msg})
		}
	}
	// send a status message as ack
	if !fromMe {
		g.sendStatus(addr)
	}
	return err
}

// Exec is the function that the gossiper uses to execute the handler for a StatusMessage
func (msg *StatusPacket) Exec(g *Gossiper, addr *net.UDPAddr) error {
	src := addr.String()
	//log.Lvlf1("STATUS from %s %s\n", src, statusString(msg.Want))

	// update new peer
	err := g.AddAddresses(src)
	g.nodes.print()

	// compare status with my status
	upToDate, sendStatus, rumorsToSend := g.vc.compareStatus(g, msg.Want)

	// check if it's an ACK
	ackedRumor, isAck := g.pm.ackPendingForPeer(msg, src)

	if upToDate {
		//log.Lvlf1("IN SYNC WITH %s\n", src)
		if isAck && rand.Int()%2 == 0 { // continue rumermongering if it was an ACK with proba 0.5
			if dst, ok := g.sendToRandomExcept(GossipPacket{Rumor: &ackedRumor}, src); ok {
				//log.Lvlf1("FLIPPED COIN sending rumor to %s\n", dst)
				g.pm.add(g, PendingMessage{dst, ackedRumor})
			}
		}
	}
	// if newer rumors, send them one by one
	for _, rumor := range rumorsToSend {
		pkt := GossipPacket{Rumor: &rumor}
		g.sendPacket(pkt, src)
	}

	// if not up to date, send own status
	if sendStatus {
		g.sendStatus(addr)
	}

	return err
}

// Exec is the function that the gossiper uses to execute the handler for a PrivateMessage
func (msg *PrivateMessage) Exec(g *Gossiper, addr *net.UDPAddr) error {
	// update new peer
	err := g.AddAddresses(addr.String())
	g.nodes.print()

	if msg.Destination == g.name { // arrived to destination !
		//log.Lvlf1("PRIVATE origin %s hop-limit %d contents %s\n", msg.Origin, msg.HopLimit, msg.Text)
		return err
	} else if msg.HopLimit == 0 { // hop limit reached, discard
		return err
	} else { // forward message if we can
		msg.HopLimit--
		pkt := GossipPacket{
			Private: msg,
		}

		dstAddr, ok := g.routes.nextHop(msg.Destination)
		if ok {
			g.sendPacket(pkt, dstAddr)
		}
	}
	return err
}

// Exec is the function that the gossiper uses to execute the handler for a DataRequest
func (msg *DataRequest) Exec(g *Gossiper, addr *net.UDPAddr) error {

	err := g.AddAddresses(addr.String())

	if msg.Destination == g.GetIdentifier() {
		//retrieve the data requested and compute hash
		data := g.metahash.QueryMetaHash(hex.EncodeToString(msg.HashValue))
		hash := sha256.Sum256(data)

		//craft reply
		reply := &DataReply{Data: data, Origin: g.GetIdentifier(), Destination: msg.Origin, HopLimit: 10, HashValue: hash[:]}
		pkt := new(GossipPacket)
		pkt.DataReply = reply

		//send back
		g.sendPacket(*pkt, addr.String())
		return err
	} else {
		//try to forward
		msg.HopLimit--
		if msg.HopLimit > 0 {
			if nextHop, found := g.routes.nextHop(msg.Destination); found {
				pkt := new(GossipPacket)
				pkt.DataRequest = msg
				g.sendPacket(*pkt, nextHop)
				return nil
			}
		}
		return errors.New("Error forwarding Data Request")
	}
}
func (msg *DataReply) Exec(g *Gossiper, addr *net.UDPAddr) error {

	err := g.AddAddresses(addr.String())

	if msg.Destination == g.GetIdentifier() {
		g.pendingReq.SendToChannel(msg)
		return err
	} else {
		//try to forward
		msg.HopLimit--
		if msg.HopLimit > 0 {
			if nextHop, found := g.routes.nextHop(msg.Destination); found {
				pkt := new(GossipPacket)
				pkt.DataReply = msg
				g.sendPacket(*pkt, nextHop)
				return nil
			}
		}
		return errors.New("Error forwarding Data Request")
	}
}

// Exec is the function that the gossiper uses to execute the handler for a SearchRequest
func (msg *SearchRequest) Exec(g *Gossiper, addr *net.UDPAddr) error {

	err := g.AddAddresses(addr.String())

	if toprocess := g.searchState.Add(msg); toprocess {

		//process locally

		results := make([]*SearchResult, 0)
		filesMap := g.metahash.ReturnMetaHash() //map[metahash]->fileInfo

		//explore files in this peer
		for metaHash, fi := range filesMap {
			r := new(SearchResult)
			for _, k := range msg.Keywords { //check if this file satisfies a keyword. If so add to result and process next file
				if strings.Contains(filepath.Base(fi.filePath), k) {
					r.FileName = filepath.Base(fi.filePath)
					r.MetaHash, _ = hex.DecodeString(metaHash)
					for i, chunk := range fi.chunks {
						if chunk != nil {
							r.ChunkMap = append(r.ChunkMap, uint32(i))
						}
					}
					break
				}
			}
			if r.ChunkMap != nil && r.FileName != "" { //it's NOT a new but empty result
				results = append(results, r)
			}
		}
		if len(results) > 0 {
			pkt := new(GossipPacket)
			pkt.SearchReply = &SearchReply{Origin: g.GetIdentifier(), Destination: msg.Origin, HopLimit: 10, Results: results}
			g.sendPacket(*pkt, addr.String())
		}
		//expanding-ring flooding

		msg.Budget--
		if msg.Budget > 0 {
			pkt := new(GossipPacket)
			pkt.SearchRequest = msg
			nodes := g.GetNodes()
			if len(nodes)-1 >= int(msg.Budget) {
				pkt.SearchRequest.Budget = 1
				nodesSentTo := make([]string, 0)
				nodesSentTo = append(nodesSentTo, addr.String())
				for B := 0; B < int(msg.Budget); B++ {
					node, _ := g.sendToRandomExcept(*pkt, nodesSentTo...)
					nodesSentTo = append(nodesSentTo, node)
				}
			} else if len(nodes)-1 > 0 {
				nodesSentTo := make([]string, 0)
				nodesSentTo = append(nodesSentTo, addr.String())
				B := int(msg.Budget)
				B_new := int(math.Ceil(float64(int(msg.Budget) / (len(nodes) - 1)))) //fairly division
				for B > 0 {
					if B >= B_new {
						B = B - B_new
					} else {
						B_new = B
						B = 0
					}
					pkt.SearchRequest.Budget = uint64(B_new)
					node, _ := g.sendToRandomExcept(*pkt, nodesSentTo...)
					nodesSentTo = append(nodesSentTo, node)
				}
			}
		}
		return err
	} else {
		return errors.New("Already have a request fresher than 0.5 seconds")
	}
}
func (msg *SearchReply) Exec(g *Gossiper, addr *net.UDPAddr) error {

	err := g.AddAddresses(addr.String())

	if msg.Destination == g.GetIdentifier() {
		//generate list of all filenames in the reply, useful for later
		filenames := make([]string, 0)
		for _, r := range msg.Results {
			filenames = append(filenames, r.FileName)
		}

		for _, result := range msg.Results {
			//update the info about this metahash
			totalmatch := g.chunkPoss.Update(hex.EncodeToString(result.MetaHash), msg.Origin, result.ChunkMap, g)
			if totalmatch {
				//find right channel in g.searches to send
				for key, ch := range g.searches {
					//for each search in progress in this peer, retrieve the list of keywords
					satisfied := 0
					keywords := strings.Split(key, ",")
					//check if each name in the reply satisfies at least one of the keywords
					//if so, then this reply is a reply for the search with those keywords
					for _, name := range filenames {
						for _, word := range keywords {
							if strings.Contains(name, word) == true {
								satisfied++
								break
							}
						}
					}
					if satisfied == len(filenames) {
						//send into the channel the hash that was a totalmatch
						ch <- result
					}
				}
			}
		}
		return err
	} else {
		msg.HopLimit--
		if msg.HopLimit > 0 {
			if nextHop, found := g.routes.nextHop(msg.Destination); found {
				pkt := new(GossipPacket)
				pkt.SearchReply = msg
				g.sendPacket(*pkt, nextHop)
				return err
			}
		}
		return errors.New("Error forwarding Search Reply")
	}
}
