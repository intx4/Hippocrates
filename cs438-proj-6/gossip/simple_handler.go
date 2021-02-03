// ========== CS-438 HW2 Skeleton ===========
// *** Implement here the handler for simple message processing ***

package gossip

import (
	"math/rand"
	"net"

	"go.dedis.ch/kyber/v3/share"

	"go.dedis.ch/cs438/hw3/gossip/project"
	"go.dedis.ch/cs438/hw3/gossip/types"
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
	//Modified for project to handle Dkg Messages
	var err error
	src := addr.String()
	fromMe := (src == g.conn.LocalAddr().String())
	//update new peer
	if !fromMe {
		err = g.AddAddresses(src)
	}
	//log.Lvlf1("RUMOR origin %s from %s ID %d contents %s\n", msg.Origin, src, msg.ID, msg.Text)
	isPaxos := msg.isPaxos()
	isDkg := msg.PkMessage != nil || msg.DealResponse != nil
	//process paxos
	if isPaxos {
		//Decrypt
		if g.cn != nil { //PROJECT
			decryptedExtra := new(types.ExtraMessage)
			decryptedExtra.Decrypt(msg.Extra, g.cnPaxosKey)
			//HANDLER FOR PAXOS
			if decryptedExtra.PaxosPrepare != nil { //PREPARE
				g.PromiseValidation(src, decryptedExtra.PaxosPrepare)
			} else if decryptedExtra.PaxosPromise != nil { //PROMISE
				g.ListenerManager(decryptedExtra)
			} else if decryptedExtra.PaxosPropose != nil { //PROPOSE
				g.AcceptValidation(decryptedExtra.PaxosPropose)
			} else if decryptedExtra.PaxosAccept != nil { //ACCEPT
				g.ListenerManager(decryptedExtra)
			} else if decryptedExtra.TLC != nil { //TLC
				g.ListenerManager(decryptedExtra)
			}
		}
	}
	// update routing table and vector clock
	isRouteRumor := (msg.isRouteRumor() && (!isPaxos && !isDkg)) //in Paxos and Dkg packets we have also txt = ""
	if !fromMe {
		g.routes.newRumor(msg, src)
	}
	isNewRumor := g.vc.newRumor(*msg)
	//g.nodes.print()
	if isNewRumor {
		if isDkg {
			//PROJECT PROCESS DKG
			if msg.DealResponse != nil {
				if g.cn != nil {
					g.cn.ProcessResponse(msg.DealResponse.Resp, msg.DealResponse.From)
				}
			}
			if msg.PkMessage != nil {
				if g.cn != nil {
					g.cn.StorePubKey(msg.PkMessage.Index, msg.PkMessage.PubKey)
				}
			}
		}
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

// // Exec is the function that the gossiper uses to execute the handler for a DataRequest
// func (msg *DataRequest) Exec(g *Gossiper, addr *net.UDPAddr) error {

// 	err := g.AddAddresses(addr.String())

// 	if msg.Destination == g.GetIdentifier() {
// 		//retrieve the data requested and compute hash
// 		data := g.metahash.QueryMetaHash(hex.EncodeToString(msg.HashValue))
// 		hash := sha256.Sum256(data)

// 		//craft reply
// 		reply := &DataReply{Data: data, Origin: g.GetIdentifier(), Destination: msg.Origin, HopLimit: 10, HashValue: hash[:]}
// 		pkt := new(GossipPacket)
// 		pkt.DataReply = reply

// 		//send back
// 		g.sendPacket(*pkt, addr.String())
// 		return err
// 	} else {
// 		//try to forward
// 		msg.HopLimit--
// 		if msg.HopLimit > 0 {
// 			if nextHop, found := g.routes.nextHop(msg.Destination); found {
// 				pkt := new(GossipPacket)
// 				pkt.DataRequest = msg
// 				g.sendPacket(*pkt, nextHop)
// 				return nil
// 			}
// 		}
// 		return errors.New("Error forwarding Data Request")
// 	}
// }
// func (msg *DataReply) Exec(g *Gossiper, addr *net.UDPAddr) error {

// 	err := g.AddAddresses(addr.String())

// 	if msg.Destination == g.GetIdentifier() {
// 		g.pendingReq.SendToChannel(msg)
// 		return err
// 	} else {
// 		//try to forward
// 		msg.HopLimit--
// 		if msg.HopLimit > 0 {
// 			if nextHop, found := g.routes.nextHop(msg.Destination); found {
// 				pkt := new(GossipPacket)
// 				pkt.DataReply = msg
// 				g.sendPacket(*pkt, nextHop)
// 				return nil
// 			}
// 		}
// 		return errors.New("Error forwarding Data Request")
// 	}
// }

// // Exec is the function that the gossiper uses to execute the handler for a SearchRequest
// func (msg *SearchRequest) Exec(g *Gossiper, addr *net.UDPAddr) error {

// 	err := g.AddAddresses(addr.String())

// 	if toprocess := g.searchState.Add(msg); toprocess {

// 		//process locally

// 		results := make([]*SearchResult, 0)
// 		filesMap := g.metahash.ReturnMetaHash() //map[metahash]->fileInfo

// 		//explore files in this peer
// 		for metaHash, fi := range filesMap {
// 			r := new(SearchResult)
// 			for _, k := range msg.Keywords { //check if this file satisfies a keyword. If so add to result and process next file
// 				if strings.Contains(filepath.Base(fi.filePath), k) {
// 					r.FileName = filepath.Base(fi.filePath)
// 					r.MetaHash, _ = hex.DecodeString(metaHash)
// 					for i, chunk := range fi.chunks {
// 						if chunk != nil {
// 							r.ChunkMap = append(r.ChunkMap, uint32(i))
// 						}
// 					}
// 					break
// 				}
// 			}
// 			if r.ChunkMap != nil && r.FileName != "" { //it's NOT a new but empty result
// 				results = append(results, r)
// 			}
// 		}
// 		if len(results) > 0 {
// 			pkt := new(GossipPacket)
// 			pkt.SearchReply = &SearchReply{Origin: g.GetIdentifier(), Destination: msg.Origin, HopLimit: 10, Results: results}
// 			g.sendPacket(*pkt, addr.String())
// 		}
// 		//expanding-ring flooding

// 		msg.Budget--
// 		if msg.Budget > 0 {
// 			pkt := new(GossipPacket)
// 			pkt.SearchRequest = msg
// 			nodes := g.GetNodes()
// 			if len(nodes)-1 >= int(msg.Budget) {
// 				pkt.SearchRequest.Budget = 1
// 				nodesSentTo := make([]string, 0)
// 				nodesSentTo = append(nodesSentTo, addr.String())
// 				for B := 0; B < int(msg.Budget); B++ {
// 					node, _ := g.sendToRandomExcept(*pkt, nodesSentTo...)
// 					nodesSentTo = append(nodesSentTo, node)
// 				}
// 			} else if len(nodes)-1 > 0 {
// 				nodesSentTo := make([]string, 0)
// 				nodesSentTo = append(nodesSentTo, addr.String())
// 				B := int(msg.Budget)
// 				B_new := int(math.Ceil(float64(int(msg.Budget) / (len(nodes) - 1)))) //fairly division
// 				for B > 0 {
// 					if B >= B_new {
// 						B = B - B_new
// 					} else {
// 						B_new = B
// 						B = 0
// 					}
// 					pkt.SearchRequest.Budget = uint64(B_new)
// 					node, _ := g.sendToRandomExcept(*pkt, nodesSentTo...)
// 					nodesSentTo = append(nodesSentTo, node)
// 				}
// 			}
// 		}
// 		return err
// 	} else {
// 		return errors.New("Already have a request fresher than 0.5 seconds")
// 	}
// }
// func (msg *SearchReply) Exec(g *Gossiper, addr *net.UDPAddr) error {

// 	err := g.AddAddresses(addr.String())

// 	if msg.Destination == g.GetIdentifier() {
// 		//generate list of all filenames in the reply, useful for later
// 		filenames := make([]string, 0)
// 		for _, r := range msg.Results {
// 			filenames = append(filenames, r.FileName)
// 		}

// 		for _, result := range msg.Results {
// 			//update the info about this metahash
// 			totalmatch := g.chunkPoss.Update(hex.EncodeToString(result.MetaHash), msg.Origin, result.ChunkMap, g)
// 			if totalmatch {
// 				//find right channel in g.searches to send
// 				for key, ch := range g.searches {
// 					//for each search in progress in this peer, retrieve the list of keywords
// 					satisfied := 0
// 					keywords := strings.Split(key, ",")
// 					//check if each name in the reply satisfies at least one of the keywords
// 					//if so, then this reply is a reply for the search with those keywords
// 					for _, name := range filenames {
// 						for _, word := range keywords {
// 							if strings.Contains(name, word) == true {
// 								satisfied++
// 								break
// 							}
// 						}
// 					}
// 					if satisfied == len(filenames) {
// 						//send into the channel the hash that was a totalmatch
// 						ch <- result
// 					}
// 				}
// 			}
// 		}
// 		return err
// 	} else {
// 		msg.HopLimit--
// 		if msg.HopLimit > 0 {
// 			if nextHop, found := g.routes.nextHop(msg.Destination); found {
// 				pkt := new(GossipPacket)
// 				pkt.SearchReply = msg
// 				g.sendPacket(*pkt, nextHop)
// 				return err
// 			}
// 		}
// 		return errors.New("Error forwarding Search Reply")
// 	}
// }

//PROJECT------------------------------------------------------------------------------------------------
func (g *Gossiper) ExecuteHandlerHippo(msg *project.HippoMsg, addr *net.UDPAddr) error {
	//Handler for project Packets
	if msg.DkgMsg != nil {
		g.DkgHandler(msg.DkgMsg, addr)
	}
	if msg.PkReq != nil { // PUB KEY REQUEST FROM PATIENT TO COTHORITY
		if g.cn != nil {
			var pkt GossipPacket
			pkt.Hippo = new(project.HippoMsg)
			pkt.Hippo.PkReply = new(project.PkReply)
			pk, granted := g.cn.GetPublicKey()
			pkb, _ := pk.MarshalBinary()
			pkt.Hippo.PkReply = &project.PkReply{Pk: pkb, Granted: granted}
			g.sendPacket(pkt, addr.String())
		}
	}
	if msg.PkReply != nil { // PUB KEY REPLY FROM COTHORITY TO PATIENT
		if g.patient != nil {
			if msg.PkReply.Granted {
				//fmt.Println("Patient pk coth", msg.PkReply.Pk)
				pk := suite.Point()
				err := pk.UnmarshalBinary(msg.PkReply.Pk)
				if err != nil {
					Log(err)
					return err
				}
				g.patient.SetPk(pk)
			}
		}
	}
	if msg.EncFileI != nil { // ENCRYPTED INFO FOR FILE TO THE COTHORITY FROM PATIENT
		if g.cn != nil {
			g.cn.AcceptFile(msg.EncFileI.EntryHash, msg.EncFileI)
		}
	}
	if msg.FileReq != nil { // REQUEST FOR A FILE TO COTHORITY FROM DOC
		if g.cn != nil {
			g.cn.ProcessFileRequest(msg.FileReq.Fr, msg.FileReq.Hmac, addr.String())
		}
	}
	if msg.PartDec != nil { //PARTIAL DEC FROM COTHORITY TO DOC
		if g.doctor != nil {
			c := suite.Point()
			a := suite.Point()
			//fmt.Println("Doctor: cn pk, c", msg.PartDec.A, msg.PartDec.C)
			errc := c.UnmarshalBinary(msg.PartDec.C)
			erra := a.UnmarshalBinary(msg.PartDec.A)
			if errc != nil && erra != nil {
				if erra != nil {
					Log(erra)
					return erra
				} else if errc != nil {
					Log(errc)
					return errc
				}
			}
			V := suite.Point()
			err := V.UnmarshalBinary(msg.PartDec.PubShareV)
			if err != nil {
				Log(err)
				return err
			}
			pubShare := &share.PubShare{I: msg.PartDec.PubShareI, V: V}
			g.doctor.CollectPartialDec(msg.PartDec.EntryHash, pubShare, c, a, msg.PartDec.Iv, msg.PartDec.EncInfo)
		}
	}
	if msg.ChunkReq != nil { //REQUEST FOR A CHUNK. DHT NODE SHOULD HANDLE

	}
	if msg.EncChunk != nil { //ENCRYPTED CHUNK SENT TO DOCTOR
		if g.doctor != nil {
			g.doctor.DecryptChunk(msg.EncChunk.Data)
		}
	}
	return nil
}
func (g *Gossiper) DkgHandler(msg *project.DkgMessage, addr *net.UDPAddr) {
	if g.cn != nil {
		if msg.DealMsg != nil {
			if msg.DealMsg.To == g.cn.index {
				//process if and only if this deal is for me
				g.cn.ProcessDeal(msg.DealMsg.Deal)
			}
		}
	}
}
