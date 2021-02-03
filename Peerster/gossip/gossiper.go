// ========== CS-438 HW3 Skeleton ===========
// *** Implement here the gossiper ***
package gossip

import (

	//"cmd/compile/internal/types"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/types"
	"go.dedis.ch/cs438/hw3/gossip/watcher"
	"go.dedis.ch/onet/v3/log"

	//HW2 imports

	"os"
)

// BaseGossipFactory provides a factory to instantiate a Gossiper
//
// - implements gossip.GossipFactory
type BaseGossipFactory struct{}

// stopMsg is used to notifier the listener when we want to close the
// connection, so that the listener knows it can stop listening.
const stopMsg = "stop"

// maximum size of a UDP packet in bytes
const maxUDPsize = 65507

// New implements gossip.GossipFactory. It creates a new gossiper.
func (f BaseGossipFactory) New(address, identifier string, antiEntropy int,
	routeTimer int, rootSharedData string, rootDownloadedFiles string,
	numParticipant, nodeIndex int, paxosRetry int) (BaseGossiper, error) {

	return NewGossiper(address, identifier, antiEntropy, routeTimer, rootSharedData, rootDownloadedFiles, numParticipant, nodeIndex, paxosRetry)
}

// Gossiper provides the functionalities to handle a distributes gossip
// protocol.
//
// - implements gossip.BaseGossiper
type Gossiper struct {
	// Folder to store data about indexed files (comes from -sharedir)
	rootSharedData string
	// Folder to store downloaded files (comes from -downdir)
	rootDownloadedFiles string
	// *** Add more here
	inWatcher       watcher.Watcher
	outWatcher      watcher.Watcher
	routes          *Routes // origin (name) -> ip:port
	Handlers        map[reflect.Type]interface{}
	addrStr         string
	addr            *net.UDPAddr
	conn            *net.UDPConn
	name            string
	nodes           *NodesList
	callback        NewMessageCallback
	antiEntropy     int
	antiEntropyChan chan bool
	rTimer          int
	rChan           chan bool
	vc              *VectorClock
	pm              *PendingMessages
	terminated      chan bool
	//HW2------------------------------------------------------------------------------------
	metahash   *MetaHash                     //see meta_hash.go
	pendingReq *PendingRequests              //see data_requests.go
	chunkPoss  *ChunkPossesion               //see chunk_possesion.go
	searches   map[string]chan *SearchResult //comma separated keywords -> chanel
	/*The idea for using this is that in AddSearchMessage the thread will receive
	  in the channel the MetaHashes of those files for which there is a total match
	  The Preprocessing is done in the handler which knows if a filename satisfies a keyword */
	searchState *SearchState //see search_state.go
	//HW3-------------------------------------------------------------------------------------
	paxosRounds    map[int]*PaxosRound //SeqID->acceptor,proposer,listener
	paxosRetry     int
	numParticipant int
	nodeIndex      int
	uniqId         *uniqIDGen //to be used inside a Paxos run
	bc             *BlockChain
}

// NewGossiper returns a Gossiper that is able to listen to the given address
// and which has the given identifier. The address must be a valid IPv4 UDP
// address. This method can panic if it is not possible to create a
// listener on that address. To run the gossip protocol, call `Run` on the
// gossiper.
func NewGossiper(address, identifier string, antiEntropy int, routeTimer int,
	rootSharedData string, rootDownloadedFiles string, numParticipant int,
	nodeIndex int, paxosRetry int) (BaseGossiper, error) {
	addr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		log.Error("Invalid ipv4 address", err)
		return nil, err
	}
	if _, err := os.Stat(rootSharedData); os.IsNotExist(err) {
		os.Mkdir(rootSharedData, os.ModeDir)
	}

	if _, err := os.Stat(rootDownloadedFiles); os.IsNotExist(err) {
		os.Mkdir(rootDownloadedFiles, os.ModeDir)
	}
	nodes := &NodesList{exists: make(map[string]bool), addrList: make([]string, 0)}
	handlers := make(map[reflect.Type]interface{})
	routes := &Routes{routes: make(map[string]*RouteStruct)}
	inWatcher := watcher.NewSimpleWatcher()
	outWatcher := watcher.NewSimpleWatcher()
	vc := &VectorClock{nextID: make(map[string]uint32), messages: make(map[string][]RumorMessage)}
	pm := &PendingMessages{pending: make(map[PendingMessage]*time.Timer)}

	metahash := new(MetaHash)
	pendingReq := new(PendingRequests)
	chunkPoss := new(ChunkPossesion)
	searches := make(map[string]chan *SearchResult)
	searchState := new(SearchState)
	//HW3
	bc := new(BlockChain)

	g := &Gossiper{
		addrStr:             address,
		addr:                addr,
		name:                identifier,
		nodes:               nodes,
		Handlers:            handlers,
		routes:              routes,
		antiEntropy:         antiEntropy,
		rTimer:              routeTimer,
		inWatcher:           inWatcher,
		outWatcher:          outWatcher,
		vc:                  vc,
		pm:                  pm,
		rootSharedData:      rootSharedData,
		rootDownloadedFiles: rootDownloadedFiles,
		metahash:            metahash.New(),
		pendingReq:          pendingReq.New(),
		chunkPoss:           chunkPoss.New(),
		searches:            searches,
		searchState:         searchState.New(),
		paxosRounds:         make(map[int]*PaxosRound),
		paxosRetry:          paxosRetry,
		numParticipant:      numParticipant,
		uniqId:              newSeqGen(nodeIndex, numParticipant),
		nodeIndex:           nodeIndex,
		bc:                  bc.New(),
	}
	//Initialize first Paxos Round
	g.paxosRounds[bc.GetLength()] = new(PaxosRound).New()

	for _, handler := range []interface{}{&SimpleMessage{}, &RumorMessage{}, &StatusPacket{}, &PrivateMessage{}, &DataRequest{}, &DataReply{}, &SearchReply{}, &SearchRequest{}} {
		err = g.RegisterHandler(handler)
		if err != nil {
			log.Fatal("failed to register", err)
		}
	}
	return g, err
}

// Run implements gossip.BaseGossiper. It starts the listening of UDP datagrams
// on the given address and starts the antientropy. This is a blocking function.
func (g *Gossiper) Run(ready chan struct{}) {
	conn, err := net.ListenUDP("udp4", g.addr)
	if err != nil {
		panic(err)
	}
	//conn.SetWriteBuffer(10 * 1024)
	g.conn = conn

	//rebuild index of file
	g.metahash.Rebuild(g.rootSharedData, g.rootDownloadedFiles)
	close(ready)

	// start tickers
	g.antiEntropyChan = startTicker(g.antiEntropy, g.antiEntropyCallback)
	if g.rTimer > 0 {
		g.rChan = startTicker(g.rTimer, g.routingCallback)
		g.routingCallback() // startup timer
	}

	// start listening on separate thread
	go func() {
		g.terminated = make(chan bool)
		defer close(g.terminated)
		for {
			buf := make([]byte, maxUDPsize)
			n, src, err := conn.ReadFromUDP(buf)
			if err != nil {
				//log.Error(err)
				continue
			}
			if text := string(buf[:n]); text == stopMsg { // check for stop message
				return
			}
			go g.handlePacket(buf, n, src)
		}
	}()
}

func (g *Gossiper) handlePacket(buf []byte, n int, src *net.UDPAddr) error {
	srcStr := src.String()
	pkt := GossipPacket{}
	err := json.Unmarshal(buf[:n], &pkt) // parse packet
	if err != nil {
		log.Error("problem parsing json", err)
		return err
	}

	go g.inWatcher.Notify(CallbackPacket{Addr: srcStr, Msg: pkt.Copy()})

	if pkt.Simple != nil { ///////////////////////////////// SIMPLE
		if g.callback != nil {
			g.callback(pkt.Simple.OriginPeerName, pkt)
		}
		err = g.ExecuteHandler(pkt.Simple, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
		g.broadcastMessageExcept(pkt, srcStr)
	} else if pkt.Rumor != nil { /////////////////////////// RUMOR
		err = g.ExecuteHandler(pkt.Rumor, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	} else if pkt.Status != nil { ////////////////////////// STATUS
		err = g.ExecuteHandler(pkt.Status, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	} else if pkt.Private != nil { ///////////////////////// PRIVATE
		err = g.ExecuteHandler(pkt.Private, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	} else if pkt.DataRequest != nil { ///////////////////// DATA REQUEST
		err = g.ExecuteHandler(pkt.DataRequest, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	} else if pkt.DataReply != nil { ///////////////////// DATA REPLY
		err = g.ExecuteHandler(pkt.DataReply, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	} else if pkt.SearchRequest != nil { ///////////////////// SEARCH REQUEST
		err = g.ExecuteHandler(pkt.SearchRequest, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	} else if pkt.SearchReply != nil { ///////////////////// SEARCH REPLY
		err = g.ExecuteHandler(pkt.SearchReply, src)
		if err != nil {
			log.Error("problem processing message", err)
			return err
		}
	}
	return nil
}

// Stop implements gossip.BaseGossiper. It closes the UDP connection
func (g *Gossiper) Stop() {
	go func(g *Gossiper) {
		if g.rTimer > 0 {
			close(g.rChan)
		}
		close(g.antiEntropyChan)
		g.pm.cleanUp()
		conn, _ := net.ListenUDP("udp4", nil)
		listener, _ := net.ResolveUDPAddr("udp4", g.conn.LocalAddr().String())
		conn.WriteToUDP([]byte(stopMsg), listener)
		<-g.terminated
		g.conn.Close()
	}(g)
}

// AddSimpleMessage implements gossip.BaseGossiper. It takes a text that will be
// spread through the gossip network with the identifier of g.
// SIMPLE -> broadcast to all
func (g *Gossiper) AddSimpleMessage(text string) {
	msg := &SimpleMessage{
		Contents:       text,
		OriginPeerName: g.name,
		RelayPeerAddr:  g.addrStr,
	}
	// create gossip packet
	pkt := GossipPacket{
		Simple: msg,
	}
	//log.Lvlf1("CLIENT MESSAGE %s\n", text)
	//log.Lvlf1("PEERS %s\n", strings.Join(g.GetNodes(), ","))
	// broadcast packet
	g.BroadcastMessage(pkt)
}

// AddMessage takes a text that will be spread through the gossip network
// with the identifier of g. It returns the ID of the message
// RUMOR -> rumormonger to random peer
func (g *Gossiper) AddMessage(text string) uint32 {
	pkt := g.vc.generateNewRumor(g.name, text)

	dst, ok := g.sendToRandom(pkt)
	if ok {
		g.pm.add(g, PendingMessage{dst, *pkt.Rumor})
	}

	return pkt.Rumor.ID
}

// AddPrivateMessage sends the message to the next hop.
func (g *Gossiper) AddPrivateMessage(text, dest, origin string, hoplimit int) {
	if dest == origin {
		log.Info("Sending private message to ourself")
		return
	}
	if hoplimit == 0 {
		log.Info("Trying to send private message with hoplimit = 0")
		return
	}
	hoplimit--
	msg := PrivateMessage{
		Origin:      origin,
		ID:          0,
		Text:        text,
		Destination: dest,
		HopLimit:    hoplimit,
	}

	pkt := GossipPacket{
		Private: &msg,
	}

	//log.Lvlf1("CLIENT MESSAGE %s dest %s\n", text, dest)

	dstAddr, ok := g.routes.nextHop(dest)
	if ok {
		g.sendPacket(pkt, dstAddr)
	}
}

// AddAddresses implements gossip.BaseGossiper. It takes any number of node
// addresses that the gossiper can contact in the gossiping network.
func (g *Gossiper) AddAddresses(addresses ...string) error {
	addressesFiltered := make([]string, 0)
	for _, addr := range addresses {
		//Paxos sends messages to ourselves
		if addr != g.conn.LocalAddr().String() {
			addressesFiltered = append(addressesFiltered, addr)
		}
	}
	return g.nodes.addAddresses(addressesFiltered)
}

// GetNodes implements gossip.BaseGossiper. It returns the list of nodes this
// gossiper knows currently in the network.
func (g *Gossiper) GetNodes() []string {
	return g.nodes.getNodes()
}

// GetDirectNodes implements gossip.BaseGossiper. It returns the list of nodes whose routes are known to this node
func (g *Gossiper) GetDirectNodes() []string {
	return g.routes.getDirectNodes()
}

// SetIdentifier implements gossip.BaseGossiper. It changes the identifier sent
// with messages originating from this gossiper.
func (g *Gossiper) SetIdentifier(id string) {
	g.name = id
}

// GetIdentifier implements gossip.BaseGossiper. It returns the currently used
// identifier for outgoing messages from this gossiper.
func (g *Gossiper) GetIdentifier() string {
	return g.name
}

// GetRoutingTable implements gossip.BaseGossiper. It returns the known routes.
func (g *Gossiper) GetRoutingTable() map[string]*RouteStruct {
	return g.routes.getRoutingTable()
}

// GetLocalAddr implements gossip.BaseGossiper. It returns the address
// (ip:port as a string) currently used to send to and receive messages
// from other peers.
func (g *Gossiper) GetLocalAddr() string {
	// Here is an implementation example that assumes storing the connection in the gossiper.
	// Adjust it to your implementation.
	//return g.conn.LocalAddr().String()
	return g.conn.LocalAddr().String()
}

// AddRoute updates the gossiper's routing table by adding a next hop for the given
// peer node
func (g *Gossiper) AddRoute(peerName, nextHop string) {
	g.routes.AddRoute(peerName, nextHop)
}

// RegisterCallback implements gossip.BaseGossiper. It sets the callback that
// must be called each time a new message arrives.
func (g *Gossiper) RegisterCallback(m NewMessageCallback) {
	g.callback = m
}

// BroadcastMessage implements gossip.BaseGossiper. It broadcasts a message to all
// known peers
func (g *Gossiper) BroadcastMessage(p GossipPacket) {
	g.broadcastMessageExcept(p, "")
}

///////////// SENDING /////////////

func (g *Gossiper) sendPacket(p GossipPacket, dst string) {
	encoded, err := json.Marshal(p)
	if err != nil {
		log.Error(err)
		return
	}
	addr, _ := net.ResolveUDPAddr("udp4", dst)
	go g.outWatcher.Notify(CallbackPacket{Addr: dst, Msg: p.Copy()})
	g.sendDataToNode(encoded, addr)
}

func (g *Gossiper) sendToRandom(pkt GossipPacket) (string, bool) {
	node, ok := g.nodes.getRandom()
	if ok {
		g.sendPacket(pkt, node)
	}
	return node, ok
}

func (g *Gossiper) sendToRandomExcept(pkt GossipPacket, peers ...string) (string, bool) {
	node, ok := g.nodes.getRandomExcept(peers)
	if ok {
		g.sendPacket(pkt, node)
	}
	return node, ok
}

func (g *Gossiper) sendStatus(dst *net.UDPAddr) {
	status := g.vc.getStatus()
	msg := StatusPacket{status}
	pkt := GossipPacket{Status: &msg}
	g.sendPacket(pkt, dst.String())
}

func (g *Gossiper) broadcastMessageExcept(p GossipPacket, except string) {
	encoded, err := json.Marshal(p)
	if err != nil {
		log.Error(err)
		return
	}
	nodes := g.GetNodes()
	for _, node := range nodes {
		if node != except {
			dst, _ := net.ResolveUDPAddr("udp4", node)
			go g.outWatcher.Notify(CallbackPacket{Addr: node, Msg: p.Copy()}) // TODO: these packets too ?
			go g.sendDataToNode(encoded, dst)
		}
	}
}

func (g *Gossiper) sendDataToNode(data []byte, dst *net.UDPAddr) {
	g.conn.WriteToUDP(data, dst)
}

//HW2---------------------------------------------------------------------------------------------------

// AddSearchMessage broadcasts search requests and waits for responses.
// Returns after:
//	getting a number of total matches
//	5 seconds for budget != 0
//	as soon as budget is > 32 for budget = 0
func (g *Gossiper) AddSearchMessage(origin string, budget uint64, keywords []string) {

	//Creates key as comma separated list of keywords. It's a string
	key := ""
	for _, k := range keywords {
		key += k
		key += ","
	}
	g.searches[key] = make(chan *SearchResult)
	metahashes := make([]string, 0)
	names := make([]string, 0)
	threshold := make(chan bool)

	if budget == 0 {
		//Start with budget = 2 and double every second in a new request
		budget = 2
		pkt := new(GossipPacket)
		pkt.SearchRequest = &SearchRequest{Origin: g.GetIdentifier(), Budget: budget, Keywords: keywords}
		g.BroadcastMessage(*pkt)
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for budget <= 32 {
				select {
				case <-ticker.C:
					budget = 2 * budget
					pkt.SearchRequest.Budget = budget
					g.BroadcastMessage(*pkt)
				case result := <-g.searches[key]:
					metahashes = append(metahashes, hex.EncodeToString(result.MetaHash))
					names = append(names, result.FileName)
					if len(metahashes) == 2 {
						ticker.Stop()
						threshold <- true
					}
				}
			}
			threshold <- true
		}()
		<-threshold //waits until we receive 2 total matches or after budget >= 32
		return
	} else {
		pkt := new(GossipPacket)
		pkt.SearchRequest = &SearchRequest{Origin: g.GetIdentifier(), Budget: budget, Keywords: keywords}
		g.BroadcastMessage(*pkt)
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			for {
				select {
				//these are the 2 files for which we have a total match
				case result := <-g.searches[key]:
					metahashes = append(metahashes, hex.EncodeToString(result.MetaHash))
					if len(metahashes) == 2 { //threshold = 2
						threshold <- true
					}
				case <-ticker.C:
					threshold <- true
					return
				}
			}
		}()
		return
	}
}

// GetRemoteChunksPossession returns locally stored information (obtained via search) about which chunks each node has for
// the given file metahash. The key of the return map is a peer's ID and the value is a list of chunk IDs.
func (g *Gossiper) GetRemoteChunksPossession(metaHash string) map[string][]uint32 {
	return g.chunkPoss.GetRemoteChunksPossession(metaHash)
}

// IndexShares indexes the files specified in filenames
func (g *Gossiper) IndexShares(shares string) {

	files_paths := strings.Split(shares, ",")
	for _, path := range files_paths {
		fi, err := os.Stat(path)
		if err != nil {
			log.Error("Impossible to retrieve stat for: ", path)
		}
		if fi.Size() > 2*1024*1000 { //2Mib
			log.Error(path, " too big!")
		} else {
			g.metahash.GenerateMetaFile(g, path, g.rootSharedData)
		}
	}
}

// GetIndexedFiles returns a list of Files currently indexed by the gossiper
func (g *Gossiper) GetIndexedFiles() []*File {
	return g.metahash.GetIndexedFiles()
}

// DownloadFile firsts requests a metafile of a given file and then retrieves it
// chunk by chunk
func (g *Gossiper) DownloadFile(dest, filename, metahash string) {
	var err error
	if dest != "" {
		//retrieve MetaFile either locally or online
		metaFile := make([]byte, 0)
		metaFile = g.metahash.QueryMetaHash(metahash)
		if metaFile == nil {
			metaFile, err = g.RetrieveMetaFile(dest, metahash)
			if err != nil {
				log.Error("Error retrieving MetaFile")
				return
			}
			g.metahash.AddToMetaHash(g.rootDownloadedFiles, filename, metahash, metaFile)
		}
		//retrieve hashes of chunks from metafile
		chunkHashes := make([]string, 0)
		var buf [32]byte
		for i, b := range metaFile {
			buf[i%32] = b
			if (i+1)%32 == 0 {
				hashStr := hex.EncodeToString(buf[:])
				chunkHashes = append(chunkHashes, hashStr)
			}
		}

		g.RetrieveFileChunks(g.rootDownloadedFiles, filename, dest, chunkHashes)
	} else {
		//use result of previous searches

		chunksInPoss := g.chunkPoss.GetRemoteChunksPossession(metahash)
		if chunksInPoss != nil {
			//retrieve MetaFile from one of the peers who has at least one chunk of the file, at random
			chunksList := make([]string, 0)
			for peerId, _ := range chunksInPoss {
				chunksList = append(chunksList, peerId)
			}

			metaFile, ok := g.chunkPoss.RetrieveMetaFile(metahash)
			if !ok {
				var err error
				err = errors.New("")
				for err != nil {
					randi := rand.Intn(len(chunksList))
					dest := chunksList[randi]
					metaFile, err = g.RetrieveMetaFile(dest, metahash)
					if err != nil {
						log.Error("Error retrieving MetaFile")
					}
				}
			}
			g.metahash.AddToMetaHash(g.rootDownloadedFiles, filename, metahash, metaFile)

			//get chunks hashes from metafile
			chunkHashes := make([]string, 0)
			var buf [32]byte
			for i, b := range metaFile {
				buf[i%32] = b
				if (i+1)%32 == 0 {
					hashStr := hex.EncodeToString(buf[:])
					chunkHashes = append(chunkHashes, hashStr)

				}
			}
			g.RetrieveFileChunksFromSearch(g.rootDownloadedFiles, filename, chunksInPoss, chunkHashes)
		}
	}
}

// Retrieves the metafile with the given hash from the dest node
func (g *Gossiper) RetrieveMetaFile(dest string, hash string) ([]byte, error) {
	hashbytes, _ := hex.DecodeString(hash)
	pkt := new(GossipPacket)
	pkt.DataRequest = &DataRequest{Origin: g.GetIdentifier(), Destination: dest, HopLimit: 10, HashValue: hashbytes}
	p, ticket := g.pendingReq.AddRequest(pkt.DataRequest)
	//send data req with exponential backoff
	i := float64(1)
	ticker := time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
	found := false
	nextHop := ""
	for !found {
		//Hopefully with route rumors we will eventually have a nextHop
		nextHop, found = g.routes.nextHop(dest)
	}
	g.sendPacket(*pkt, nextHop)

	for i < 6 {
		select {
		case <-ticker.C:
			i++
			ticker = time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
			g.sendPacket(*pkt, dest)
		case reply := <-p.replychan:
			//ticker.Stop()
			hashOfData := sha256.Sum256(reply.Data)
			if hex.EncodeToString(reply.HashValue) == hex.EncodeToString(hashOfData[:]) {
				g.pendingReq.RemoveRequest(ticket)
				return reply.Data, nil
			} else {
				i++
				ticker = time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
				g.sendPacket(*pkt, dest)
			}
		}
	}
	return nil, errors.New("No response")
}

//Retrieves file chunks from MetaFile given the hashes of chunk in the metafile
func (g *Gossiper) RetrieveFileChunks(downloadDir, filename, dest string, hashes []string) {
	for _, hash := range hashes {
		hashbytes, _ := hex.DecodeString(hash)
		pkt := new(GossipPacket)
		pkt.DataRequest = &DataRequest{Origin: g.GetIdentifier(), Destination: dest, HopLimit: 10, HashValue: hashbytes}
		p, ticket := g.pendingReq.AddRequest(pkt.DataRequest)
		//send data req with exponential backoff
		i := float64(1)
		ticker := time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
		if nextHop, found := g.routes.nextHop(dest); found {
			g.sendPacket(*pkt, nextHop)
		} else {
			return
		}
		done := false
		for i < 6 && !done {
			select {
			case <-ticker.C:
				i++
				ticker = time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
				g.sendPacket(*pkt, dest)
			case reply := <-p.replychan:
				ticker.Stop()
				hashOfData := sha256.Sum256(reply.Data)
				if hex.EncodeToString(reply.HashValue) == hex.EncodeToString(hashOfData[:]) {
					g.pendingReq.RemoveRequest(ticket)
					//store the chunk
					g.metahash.AddToMetaHash(downloadDir, filename, hash, reply.Data)
					done = true // to exit for loop
					break
				} else {
					i++
					ticker = time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
					g.sendPacket(*pkt, dest)
				}
			}
		}
		if i >= 6 {
			log.Error("No response")
			return
		}
	}
}

//Variation fo RetrieveFileChunks which uses information of previous searches
func (g *Gossiper) RetrieveFileChunksFromSearch(downloadDir, filename string, chunksInPoss map[string][]uint32, hashes []string) {
	dest := ""
	for index, hash := range hashes {
		hashbytes, _ := hex.DecodeString(hash)
		pkt := new(GossipPacket)
		found := false
		//query the chunks possesed by the peers to find a suitable dest for the request
		for peerid, chunks := range chunksInPoss {
			for _, id := range chunks {
				if index == int(id) {
					dest = peerid
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		pkt.DataRequest = &DataRequest{Origin: g.GetIdentifier(), Destination: dest, HopLimit: 10, HashValue: hashbytes}
		p, ticket := g.pendingReq.AddRequest(pkt.DataRequest)
		//send data req with exponential backoff
		i := float64(1)
		ticker := time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
		if nextHop, found := g.routes.nextHop(dest); found {
			g.sendPacket(*pkt, nextHop)
		} else {
			return
		}
		done := false
		for i < 6 && !done {
			select {
			case <-ticker.C:
				i++
				ticker = time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
				g.sendPacket(*pkt, dest)
			case reply := <-p.replychan:
				ticker.Stop()
				hashOfData := sha256.Sum256(reply.Data)
				if hex.EncodeToString(reply.HashValue) == hex.EncodeToString(hashOfData[:]) {
					g.pendingReq.RemoveRequest(ticket)
					//store the chunk
					g.metahash.AddToMetaHash(downloadDir, filename, hash, reply.Data)
					done = true
					break
				} else {
					i++
					ticker = time.NewTicker(time.Duration(int(math.Pow(2, i))) * time.Second)
					g.sendPacket(*pkt, dest)
				}
			}
		}
		if i >= 6 {
			log.Error("No response")
			return
		}
	}
}

// RemoveChunkFromLocal removes the chunk with the given hash from the local index of available chunks.
func (g *Gossiper) RemoveChunkFromLocal(chunkHash string) {
	g.metahash.RemoveChunk(chunkHash)
}

//HW3-----------------------------------------------------------------------------------------

func (g *Gossiper) PaxosRun(FileName, metaHash string, boxNum int, prevBlkHash []byte) {
	//Initialize uniqId
	g.uniqId = newSeqGen(g.nodeIndex, g.numParticipant)
	ID := g.uniqId.currentID
	proposer := g.paxosRounds[boxNum].p

	consensus := false
	//consensusTlc := false
	//acceptedValue := types.Block{}

	//try with random backoff
	for !consensus {
		//phase 1
		proposer.SignalPhase(1)

		mh, _ := hex.DecodeString(metaHash)

		//start with my proposed value
		majAccValue := types.Block{BlockNumber: boxNum, PreviousHash: prevBlkHash, Metahash: mh, Filename: FileName}

		extra := new(types.ExtraMessage)
		extra.PaxosPrepare = new(types.PaxosPrepare)
		extra.NewPaxosMsg(ID, -1, boxNum, majAccValue)
		pkt := g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)

		proposer.promiseDS[ID] = make(chan *types.PaxosPromise, g.numParticipant)

		backoff := 1
		count := 0
		threshold := int(math.Floor(float64(g.numParticipant/2) + 1))
		//go g.BroadcastMessage(pkt)
		go g.sendToRandom(pkt)
		go g.sendPacket(pkt, g.conn.LocalAddr().String())

		ticker := time.NewTicker(time.Duration(backoff*g.paxosRetry) * time.Second)

		for count < threshold {
			select {
			case promise := <-proposer.promiseDS[ID]:
				count++
				log.LLvlf1("%s received promise IDp = %d Seq = %d", g.name, ID, boxNum)
				if promise.IDa != -1 {
					majAccValue = promise.Value
				}
			case <-ticker.C:
				//check consensusTlc
				if consensusTlc := proposer.CheckConsensusTlc(); consensusTlc {
					log.LLvlf1("Consensus Tlc Seq %d for %s", boxNum, g.name)
					return
				}
				g.uniqId.GetNext()
				ID = g.uniqId.currentID
				count = 0

				extra := new(types.ExtraMessage)
				extra.PaxosPrepare = new(types.PaxosPrepare)
				extra.NewPaxosMsg(ID, -1, boxNum, majAccValue)
				pkt := g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)

				proposer.promiseDS[ID] = make(chan *types.PaxosPromise, g.numParticipant)
				//random backoff
				backoff = rand.Intn(g.numParticipant) + 1
				//go g.BroadcastMessage(pkt)
				go g.sendToRandom(pkt)
				go g.sendPacket(pkt, g.conn.LocalAddr().String())

				ticker = time.NewTicker(time.Duration(g.paxosRetry*backoff) * time.Second)
			default:
				if consensusTlc := proposer.CheckConsensusTlc(); consensusTlc {
					log.LLvlf1("Consensus Tlc Seq %d for %s", boxNum, g.name)
					return
				}
			}
		}
		//check consensus before starting phase II
		if consensusTlc := proposer.CheckConsensusTlc(); consensusTlc {
			log.LLvlf1("Consensus Tlc %d for %s", boxNum, g.name)
			return
		}
		//phase II
		log.LLvlf1("%s going to phase 2 in Box %d", g.name, boxNum)
		proposer.SignalPhase(2)

		proposer.acceptDS[ID] = make(chan types.Block, g.numParticipant)

		extra = new(types.ExtraMessage)
		extra.PaxosPropose = new(types.PaxosPropose)
		extra.NewPaxosMsg(ID, -1, boxNum, majAccValue)
		pkt = g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)

		//go g.BroadcastMessage(pkt)
		go g.sendToRandom(pkt)
		go g.sendPacket(pkt, g.conn.LocalAddr().String())

		backoff = 1
		ticker = time.NewTicker(time.Duration(g.paxosRetry) * time.Second)
		endPhase2 := false
		for !endPhase2 {
			select {
			//collecting enough accepts is handled by the listener
			//case acceptedValue = <-g.proposer.acceptDS[ID]:
			case <-proposer.acceptDS[ID]:
				log.LLvlf1("%s reached accept consensus in box %d", g.name, boxNum)
				consensus = true
				endPhase2 = true
				break
			case <-ticker.C:
				if consensusTlc := proposer.CheckConsensusTlc(); consensusTlc {
					log.LLvlf1("Consensus Tlc Seq %d for %s", boxNum, g.name)
					return
				}
				g.uniqId.GetNext()
				endPhase2 = true
				break
			default:
				if consensusTlc := proposer.CheckConsensusTlc(); consensusTlc {
					log.LLvlf1("Consensus Tlc Seq %d for %s", boxNum, g.name)
					return
				}
			}
		}
	} //end for
	proposer.SignalPhase(0)
}

func (g *Gossiper) PromiseValidation(addr string, prepareMsg *types.PaxosPrepare) {
	IDp := prepareMsg.ID
	SeqId := prepareMsg.PaxosSeqID

	if _, found := g.paxosRounds[SeqId]; !found {
		log.Error("This node is Paxos-behind")
		return
	}

	g.paxosRounds[SeqId].a.mux.Lock()
	defer g.paxosRounds[SeqId].a.mux.Unlock()

	g.paxosRounds[SeqId].l.mux.RLock()
	consensusReached := g.paxosRounds[SeqId].l.consensusOnceAccepts
	g.paxosRounds[SeqId].l.mux.RUnlock()

	acceptor := g.paxosRounds[SeqId].a
	if !consensusReached {
		if IDp <= acceptor.highestIDp {
			log.LLvlf1("%s received prepare IDp = %d Seq = %d. Ignoring", g.name, IDp, SeqId)
			return //just ignore
		} else {
			log.LLvlf1("%s received prepare IDp = %d Seq = %d. Promising", g.name, IDp, SeqId)
			acceptor.highestIDp = IDp
			if acceptor.highestIDa == -1 {
				//I haven't accepted any value yet
				extra := new(types.ExtraMessage)
				extra.PaxosPromise = new(types.PaxosPromise)
				extra.NewPaxosMsg(IDp, -1, SeqId, types.Block{})
				pkt := g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)
				go g.sendPacket(pkt, addr)
			} else {
				//I've accepted something
				extra := new(types.ExtraMessage)
				extra.PaxosPromise = new(types.PaxosPromise)
				extra.NewPaxosMsg(acceptor.highestIDp, acceptor.highestIDa, SeqId, acceptor.highestIdaValue)
				pkt := g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)
				//pkt.Rumor.Extra.PaxosPromise.IDa = acceptor.highestIDa
				go g.sendPacket(pkt, addr)
			}
		}
	}
}

func (g *Gossiper) AcceptValidation(proposeMsg *types.PaxosPropose) {
	IDp := proposeMsg.ID
	SeqId := proposeMsg.PaxosSeqID

	if _, found := g.paxosRounds[SeqId]; !found {
		log.Error("This node is Paxos-behind")
		return
	}

	g.paxosRounds[SeqId].a.mux.Lock()
	defer g.paxosRounds[SeqId].a.mux.Unlock()

	g.paxosRounds[SeqId].l.mux.RLock()
	consensusReached := g.paxosRounds[SeqId].l.consensusOnceAccepts
	g.paxosRounds[SeqId].l.mux.RUnlock()

	acceptor := g.paxosRounds[SeqId].a
	if !consensusReached {
		if IDp < acceptor.highestIDp {
			return
			log.LLvlf1("%s received proposal IDp = %d Seq = %d. Ignoring", g.name, IDp, SeqId)
		} else {
			log.LLvlf1("%s received proposal IDp = %d Seq = %d. ACCEPTED", g.name, IDp, SeqId)
			acceptor.highestIDp = IDp
			acceptor.highestIDa = IDp
			acceptor.highestIdaValue = proposeMsg.Value
			//lastBlockHash, _ := g.bc.GetLastBlock()
			//lastBlockHashBytes, _ := hex.DecodeString(lastBlockHash)
			extra := new(types.ExtraMessage)
			extra.PaxosAccept = new(types.PaxosAccept)
			extra.NewPaxosMsg(IDp, IDp, SeqId, acceptor.highestIdaValue)
			pkt := g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)
			go g.BroadcastMessage(pkt)
			//go g.sendToRandom(pkt)
			go g.sendPacket(pkt, g.conn.LocalAddr().String())
		}
	}
}

func (g *Gossiper) ListenerManager(msg *types.ExtraMessage) {

	if msg.PaxosAccept != nil { // ACCEPT----------------------
		//handle accept
		acceptMsg := msg.PaxosAccept
		SeqId := acceptMsg.PaxosSeqID

		if _, found := g.paxosRounds[SeqId]; !found {
			log.Error("This node is Paxos-behind")
			return
		}

		listener := g.paxosRounds[SeqId].l
		proposer := g.paxosRounds[SeqId].p
		key := acceptMsg.ID
		log.LLvlf1("%s received accept ID = %d Seq = %d", g.name, acceptMsg.ID, SeqId)

		consensus, once := listener.UpdateEntry_Accepts(key, g.numParticipant)
		if consensus && once {
			//flag consensus to proposer
			if ackchan, found := proposer.acceptDS[acceptMsg.ID]; found && proposer.GetPhase() == 2 {
				log.LLvlf1("%s flagging consesus accept to proposer at box %d", g.name, SeqId)
				ackchan <- acceptMsg.Value
			}
			TLC := new(types.TLC)
			TLC.Block = acceptMsg.Value
			//need proper previous hash
			previousHash, _ := g.bc.GetLastBlock()
			previousHashBytes := make([]byte, 32)
			if previousHash != "" {
				previousHashBytes, _ = hex.DecodeString(previousHash)
			}
			TLC.Block.PreviousHash = previousHashBytes
			extra := new(types.ExtraMessage)
			extra.TLC = TLC
			pkt := g.vc.generateNewRumor_Paxos(g.GetIdentifier(), "", extra)
			log.LLvlf1("%s sending TLC at box %d", g.name, SeqId)
			go g.BroadcastMessage(pkt)
			//go g.sendToRandom(pkt)
			go g.sendPacket(pkt, g.conn.LocalAddr().String())
		}
	} else if msg.PaxosPromise != nil { // PROMISE---------------------
		promiseMsg := msg.PaxosPromise
		SeqId := promiseMsg.PaxosSeqID

		if _, found := g.paxosRounds[SeqId]; !found {
			log.Error("This node is Paxos-behind")
			return
		}
		g.paxosRounds[SeqId].l.mux.RLock()
		consensusReached := g.paxosRounds[SeqId].l.consensusOnceAccepts
		g.paxosRounds[SeqId].l.mux.RUnlock()
		if !consensusReached {
			proposer := g.paxosRounds[SeqId].p
			//log.LLvlf1("%s received promise IDp = %d Seq = %d", g.name, promiseMsg.IDp, SeqId)
			if ackchan, found := proposer.promiseDS[promiseMsg.IDp]; found && proposer.GetPhase() == 1 {
				log.LLvlf1("%s sending promise to proposer at box %d", g.name, SeqId)
				ackchan <- promiseMsg
			}
		}
	} else if msg.TLC != nil { //TLC--------------------------
		TLC := msg.TLC
		SeqId := TLC.Block.BlockNumber
		if _, found := g.paxosRounds[SeqId]; !found {
			log.Error("This node is Paxos-behind")
			return
		}
		listener := g.paxosRounds[SeqId].l
		proposer := g.paxosRounds[SeqId].p
		log.LLvlf1("%s received TLC SeqID = %d", g.name, SeqId)
		consensus, once := listener.UpdateEntry_Tlc(TLC.Block.BlockNumber, g.numParticipant, TLC.Block)
		if consensus && once {
			log.LLvlf1("%s flagging TLC consensus to proposer at box %d", g.name, SeqId)
			//listener will add the block to the blockchain
			go g.bc.AddBlock(TLC.Block, g)
			go proposer.SignalConsensusTlc()
		}
	}
}

func (g *Gossiper) GetBlocks() (string, map[string]types.Block) {
	return g.bc.GetBlocks()
}

//WATCHERS------------------------------------------------------------------------------------
// Watch implements gossip.BaseGossiper. It returns a chan populated with new
// incoming packets
func (g *Gossiper) Watch(ctx context.Context, fromIncoming bool) <-chan CallbackPacket {
	w := g.inWatcher

	if !fromIncoming {
		w = g.outWatcher
	}

	o := &observer{
		ch: make(chan CallbackPacket),
	}

	w.Add(o)

	go func() {
		<-ctx.Done()
		// empty the channel
		o.terminate()
		w.Remove(o)
	}()

	return o.ch
}

// - implements watcher.observable
type observer struct {
	sync.Mutex
	ch      chan CallbackPacket
	buffer  []CallbackPacket
	closed  bool
	running bool
}

func (o *observer) Notify(i interface{}) {
	o.Lock()
	defer o.Unlock()

	if o.closed {
		return
	}

	if o.running {
		o.buffer = append(o.buffer, i.(CallbackPacket))
		return
	}

	select {
	case o.ch <- i.(CallbackPacket):

	default:
		// The buffer size is not controlled as we assume the event will be read
		// shortly by the caller.
		o.buffer = append(o.buffer, i.(CallbackPacket))

		o.checkSize()

		o.running = true

		go o.run()
	}
}

func (o *observer) run() {
	for {
		o.Lock()

		if len(o.buffer) == 0 {
			o.running = false
			o.Unlock()
			return
		}

		msg := o.buffer[0]
		o.buffer = o.buffer[1:]

		o.Unlock()

		// Wait for the channel to be available to writings.
		o.ch <- msg
	}
}

func (o *observer) checkSize() {
	const warnLimit = 1000
	if len(o.buffer) >= warnLimit {
		log.Warn("Observer queue is growing insanely")
	}
}

func (o *observer) terminate() {
	o.Lock()
	defer o.Unlock()

	o.closed = true

	if o.running {
		o.running = false
		o.buffer = nil

		// Drain the message in transit to close the channel properly.
		select {
		case <-o.ch:
		default:
		}
	}

	close(o.ch)
}

// An example of how to send an incoming packet to the Watcher
// g.inWatcher.Notify(CallbackPacket{Addr: addrStr, Msg: gossipPacket.Copy()})
