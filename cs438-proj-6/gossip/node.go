package gossip

//we got some inspiration from https://github.com/scott-he/dht-2/ for some part of the DHT.
import (
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"go.dedis.ch/cs438/hw3/gossip/project"
)

type FingerUpdate struct {
	I    int
	Addr string
}

type JoinAction int

const (
	NumFingers                     = 3
	NumSuccessors                  = 3
	StabilisationTicker            = 1
	FileTicker                     = 3 * 60
	Predecessor         JoinAction = iota
	Successor
)

type Finger struct {
	Start *big.Int
	Addr  string
}

type Node struct {
	id   *big.Int
	addr string

	Predecessor string
	Fingers     [NumFingers]Finger
	Successors  [NumSuccessors]string
	tableMut    sync.RWMutex

	dataMut sync.RWMutex
	data    map[string]*project.DhtVal

	listener  net.Listener
	listening bool
	done      chan struct{}
}

// compute the start key of all fingers
// essentially the first finger is always the successor
// for the other fingers it depends and we deviate from the chord paper
// the last finger is always at least half the ring away
// each finger before that halves that distance
// the amount of fingers can be changed freely by modifying the constants above
func (n *Node) ComputeFingers() {
	// mask is the max value of a key (160 bits, size of SHA1)
	mask := big.NewInt(0).Lsh(big.NewInt(1), 160)
	mask.Sub(mask, big.NewInt(1))

	// divide by 2, that is the distance for the last finger
	fing := big.NewInt(0).Div(mask, big.NewInt(2))
	n.tableMut.Lock()
	for i := NumFingers - 1; i > 0; i-- {
		start := big.NewInt(0).Add(n.id, fing)
		start.And(start, mask) // start = (n.id + fing) % 2^160
		n.Fingers[i].Start = start
		fing.Div(fing, big.NewInt(2)) // fing = fing / 2
	}
	// first finger is successor
	n.Fingers[0].Start = big.NewInt(0).Add(n.id, big.NewInt(1))
	n.tableMut.Unlock()
}

// find the successor of a certain key
func (n *Node) FindSuccessor(id *big.Int) (string, error) {
	pred, err := n.FindPredecessor(id)
	if err != nil {
		return "", errors.New("FindSuccessor: Could not find predecessor")
	}
	succ, err := RPCGetSuccessor(pred)
	if err != nil {
		return "", errors.New("FindSuccessor: Could not ask for successor")
	}
	return succ, nil
}

// find the predecessor of a certain key
func (n *Node) FindPredecessor(id *big.Int) (string, error) {
	n.tableMut.RLock()
	mySucc := AddrToInt(n.Fingers[0].Addr)
	n.tableMut.RUnlock()
	if IsInBetweenLeftExclusive(id, n.id, mySucc) {
		return n.addr, nil
	}
	node := n.ClosestPrecedingFinger(id)

	for {
		succ, err := RPCGetSuccessor(node)

		if err != nil {
			return "", errors.New("FindPredecessor: Could not get successor")
		}
		if IsInBetweenLeftExclusive(id, AddrToInt(node), AddrToInt(succ)) {
			break
		}
		node, err = RPCClosestPrecedingFinger(node, id)
		if err != nil {
			return "", errors.New("FindPredecessor: Could not contact node")
		}
	}

	return node, nil
}

// find the closest preceding finger for a key
func (n *Node) ClosestPrecedingFinger(id *big.Int) string {
	n.tableMut.RLock()
	defer n.tableMut.RUnlock()
	for i := NumFingers - 1; i >= 0; i-- {
		if IsInBetweenBothExclusive(AddrToInt(n.Fingers[i].Addr), n.id, id) {
			return n.Fingers[i].Addr
		}
	}
	return ""
}

func (n *Node) InitFingers(join string) error {
	n.tableMut.RLock()
	mySucc := n.Fingers[0].Start
	n.tableMut.RUnlock()
	succ, err := RPCFindSuccessor(join, mySucc)
	if err != nil {
		return errors.New("InitFingers: Could not contact join address")
	}

	pred, err := RPCGetPredecessor(succ)
	if err != nil || pred == "" {
		return errors.New("InitFingers: Could not get predecessor from successor")
	}

	n.tableMut.Lock()
	n.Fingers[0].Addr = succ
	n.Predecessor = pred
	n.tableMut.Unlock()

	err = RPCNotifyPredecessorJoin(succ, n.addr)
	if err != nil {
		return errors.New("InitFingers: Could not notify successor")
	}

	for i := 0; i < NumFingers-1; i++ {
		n.tableMut.RLock()
		start := n.Fingers[i+1].Start
		addr := n.Fingers[i].Addr
		n.tableMut.RUnlock()
		if IsInBetweenRightExclusive(start, n.id, AddrToInt(addr)) {
			n.tableMut.Lock()
			n.Fingers[i+1].Addr = addr
			n.tableMut.Unlock()
		} else {
			succ, err := RPCFindSuccessor(join, start)
			if err != nil {
				return errors.New("InitFingers: Could not ask join address for finger")
			}
			n.tableMut.Lock()
			n.Fingers[i+1].Addr = succ
			n.tableMut.Unlock()
		}
	}
	return nil
}

// update other nodes' fingers if we should be in their finger table after we join
func (n *Node) UpdateOthers() {
	for i := NumFingers - 1; i <= 0; i-- {
		mask := big.NewInt(0).Lsh(big.NewInt(1), 160)
		mask.Sub(mask, big.NewInt(1))

		fing := big.NewInt(0).Div(mask, big.NewInt(2))
		for i := NumFingers - 1; i >= 0; i-- {
			start := big.NewInt(0).Add(n.id, mask)
			start.Sub(start, fing)
			start.And(start, mask)

			pred, err := n.FindPredecessor(start)
			if err == nil {
				// no problem if we could not contact the node
				// he will find out about us eventually through stabilisation
				RPCUpdateFingerTable(pred, &FingerUpdate{i, n.addr})
			}

			fing.Div(fing, big.NewInt(2))
		}
	}
}

// occasionally check on our predecessor and successors
// fix if they are wrong or dead
func (n *Node) Stabilize() {
	// check if our predecessor is still alive
	n.tableMut.RLock()
	pred := n.Predecessor
	n.tableMut.RUnlock()
	if pred != "" {
		_, err := RPCGetPredecessor(pred)
		if err != nil {
			n.tableMut.Lock()
			n.Predecessor = ""
			n.tableMut.Unlock()
		}
	}
	// try to contact our successor and ask for his predecessor
	n.tableMut.RLock()
	succ := n.Fingers[0].Addr
	n.tableMut.RUnlock()
	for i := 0; ; i++ {
		predOfSucc, err := RPCGetPredecessor(succ)
		if err != nil {
			// if we cannot contact him, then we should replace him with one of our backup successors
			if i >= NumSuccessors {
				log.Fatal("DHT network is partitioned!")
			}
			n.tableMut.RLock()
			succ = n.Successors[i]
			n.tableMut.RUnlock()
			continue
		}

		// update successor with the one who worked in the end
		n.tableMut.Lock()
		n.Fingers[0].Addr = succ
		// if our successors predecessor should be our successor, update
		if i == 0 && predOfSucc != "" && IsInBetweenBothExclusive(AddrToInt(predOfSucc), n.id, AddrToInt(n.Fingers[0].Addr)) {
			n.Fingers[0].Addr = predOfSucc
		}
		n.tableMut.Unlock()
		// remind our successor that we exist (or notify if we just changed successors)
		n.tableMut.RLock()
		succ := n.Fingers[0].Addr
		n.tableMut.RUnlock()
		RPCNotifyPredecessorJoin(succ, n.addr)

		// update the backup successor table
		n.tableMut.RLock()
		succ = n.Fingers[0].Addr
		n.tableMut.RUnlock()
		next := succ
		for i := 0; i < NumSuccessors; i++ {
			next, err = RPCGetSuccessor(next)
			if err != nil {
				break
			}
			n.tableMut.Lock()
			n.Successors[i] = next
			n.tableMut.Unlock()
		}
		break
	}
}

// Occasionally pick a random finger and update it
func (n *Node) FixFingers() {
	i := rand.Intn(NumFingers-1) + 1
	finger, err := n.FindSuccessor(n.Fingers[i].Start)
	if err == nil {
		n.tableMut.Lock()
		n.Fingers[i].Addr = finger
		n.tableMut.Unlock()
	}
}

// Occasionally check if keys we hold do not belong to another node
func (n *Node) FixFiles() {
	n.tableMut.RLock()
	pred := n.Predecessor
	n.tableMut.RUnlock()
	if pred != "" {
		// copy keys first so we can loop without having to keep the lock
		// avoids race condition in case a key somehow ends up back with us
		n.dataMut.RLock()
		keys := make([]string, 0, len(n.data))
		for k := range n.data {
			keys = append(keys, k)
		}
		n.dataMut.RUnlock()
		for _, key := range keys {
			keyInt := AddrToInt(key)
			// if we should not hold this key
			if !IsInBetweenLeftExclusive(keyInt, AddrToInt(pred), n.id) {
				n.dataMut.Lock()
				// retrieve it from the map and delete it
				data, ok := n.data[key]
				delete(n.data, key)
				n.dataMut.Unlock()

				if ok { // if it was still present in the map
					err := n.Put(key, data) // try to put it on the correct node
					if err != nil {
						// if that failed, restore it in our map so the data is not lost
						n.dataMut.Lock()
						n.data[key] = data
						n.dataMut.Unlock()
					}
				}
			}
		}
	}
}

// Called by another newly-joined node if it thinks it should be in our finger table
func (n *Node) UpdateFingerTable(up FingerUpdate, res *int) error {
	if up.I < 0 || up.I >= NumFingers {
		return errors.New("Nice try")
	}
	n.tableMut.Lock()
	if IsInBetweenRightExclusive(AddrToInt(up.Addr), n.id, AddrToInt(n.Fingers[up.I].Addr)) {
		n.Fingers[up.I].Addr = up.Addr
		n.tableMut.Unlock()
		RPCUpdateFingerTable(n.Predecessor, &up)
	}
	n.tableMut.Unlock()
	return nil
}

// expose rpc endpoint
func (n *Node) Listen() {
	if n.listening {
		return
	}
	rpc.Register(n)
	rpc.HandleHTTP()
	addrPort := strings.Split(n.addr, ":")
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", addrPort[1]))
	if e != nil {
		panic(e)
	}
	n.listener = l
	n.listening = true
	go http.Serve(l, nil)
}

func (n *Node) Quit() {
	n.done <- struct{}{}
	n.listener.Close()
}

// convert a SHA1 hash to a bigint key
func AddrToInt(addr string) *big.Int {
	h := sha1.New()
	h.Write([]byte(addr))
	return new(big.Int).SetBytes(h.Sum(nil))
}

func AddrsToInts(addrs []string) []*big.Int {
	intAddrs := make([]*big.Int, 0)
	for _, addr := range addrs {
		intAddrs = append(intAddrs, AddrToInt(addr))
	}
	return intAddrs
}

// id ∈ (left, right]
func IsInBetweenLeftExclusive(id *big.Int, left *big.Int, right *big.Int) bool {
	rightCmp := right.Cmp(id)
	leftCmp := left.Cmp(id)

	if right.Cmp(left) == 1 { // right > left
		// right >= id && left < id
		return rightCmp >= 0 && leftCmp == -1
	}
	// right >= id || left < id
	return rightCmp >= 0 || leftCmp == -1
}

// id ∈ [left, right)
func IsInBetweenRightExclusive(id *big.Int, left *big.Int, right *big.Int) bool {
	rightCmp := right.Cmp(id)
	leftCmp := left.Cmp(id)

	if right.Cmp(left) == 1 { // right > left
		// right > id && left <= id
		return rightCmp == -1 && leftCmp <= 0
	}
	// right > id || left <= id
	return rightCmp == -1 || leftCmp <= 0
}

// id ∈ (left, right)
func IsInBetweenBothExclusive(id *big.Int, left *big.Int, right *big.Int) bool {
	rightCmp := right.Cmp(id)
	leftCmp := left.Cmp(id)

	if right.Cmp(left) == 1 { // right > left
		// right > id && left < id
		return rightCmp == 1 && leftCmp == -1
	}
	// right > id && left < id
	return rightCmp == 1 || leftCmp == -1
}

// create a new DHT node
func NewNode(addr string) *Node {
	rand.Seed(time.Now().UnixNano())
	node := &Node{
		addr:      addr,
		id:        AddrToInt(addr),
		data:      make(map[string]*project.DhtVal),
		listener:  nil,
		listening: false,
		done:      make(chan struct{}),
	}
	node.ComputeFingers()
	return node
}

// join a DHT network, or create a new one if aNodeAddrInNetwork == ""
func (n *Node) Join(aNodeAddrInNetwork string) error {
	fmt.Println("NODE", aNodeAddrInNetwork)
	fmt.Println("SELF", n.addr)
	if aNodeAddrInNetwork != "" && aNodeAddrInNetwork != n.addr {
		// initialize our fingers
		err := n.InitFingers(aNodeAddrInNetwork)
		if err != nil {
			log.Fatal("Failed to get fingers from network! Try again")
		}
		// let others know we exist so they can update their fingers
		n.UpdateOthers()

		// ask our successor if it has keys for us
		dataArr, err := RPCMigrate(n.Fingers[0].Addr, n.addr)
		if err != nil {
			return errors.New("Key migration failed")
		}
		if dataArr != nil {
			n.dataMut.Lock()
			for _, data := range *dataArr {
				n.data[data.Key] = data.Val
			}
			n.dataMut.Unlock()
		}
	} else {
		// we are alone in the network, so we are our own successor
		// our fingers should point to ourself
		n.tableMut.Lock()
		for i := 0; i < NumFingers; i++ {
			n.Fingers[i].Addr = n.addr
		}
		n.Predecessor = n.addr
		n.tableMut.Unlock()
	}

	n.Listen()

	// occasionally stabilize, fix fingers, and move keys (files)
	stabTicker := time.NewTicker(StabilisationTicker * time.Second)
	fileTicker := time.NewTicker(FileTicker * time.Second)
	go func() {
		for {
			select {
			case <-n.done:
				return
			case <-stabTicker.C:
				n.Stabilize()
				n.FixFingers()
			case <-fileTicker.C:
				n.FixFiles()
			}
		}
	}()

	return nil
}

// called by our predecessor to notify us it joined the network
func (n *Node) NotifyPredecessorJoin(addrJoin string, response *string) error {
	n.tableMut.Lock()
	if n.Predecessor == "" || IsInBetweenLeftExclusive(AddrToInt(addrJoin), AddrToInt(n.Predecessor), n.id) {
		n.Predecessor = addrJoin
	}
	n.tableMut.Unlock()
	return nil
}

// Put a key in the hashmap
func (n *Node) Put(key string, value *project.DhtVal) error {
	dhtData := project.DhtData{Key: key, Val: value}
	keyInt := AddrToInt(key)
	succ, err := n.FindSuccessor(keyInt)
	if err != nil {
		return errors.New("Put: Failed to find key location")
	}
	if succ == n.addr {
		n.dataMut.Lock()
		n.data[key] = value
		n.dataMut.Unlock()
		return nil
	}
	err = RPCPut(succ, &dhtData)
	if err != nil {
		return err
	}
	return nil
}

// Put but callable by RPC
func (n *Node) PutHashTable(dhtData *project.DhtData, response *string) error {
	keyInt := AddrToInt(dhtData.Key)
	n.tableMut.RLock()
	pred := n.Predecessor
	n.tableMut.RUnlock()
	if IsInBetweenLeftExclusive(keyInt, AddrToInt(pred), n.id) {
		n.dataMut.Lock()
		n.data[dhtData.Key] = dhtData.Val
		n.dataMut.Unlock()
		return nil
	}
	return errors.New("PutHashTable: Invalid rpcput request")
}

// Get a key in the hashmap
func (n *Node) Get(key string) (*project.DhtVal, error) {
	keyInt := AddrToInt(key)
	succ, err := n.FindSuccessor(keyInt)
	if err != nil {
		return nil, errors.New("Get: Failed to find key location")
	}
	if succ == n.addr {
		n.dataMut.RLock()
		defer n.dataMut.RUnlock()
		return n.data[key], nil
	}
	val, err := RPCGet(succ, key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Get but callable by RPC
func (n *Node) GetHashTable(key string, val *project.DhtVal) error {
	keyInt := AddrToInt(key)
	n.tableMut.RLock()
	pred := n.Predecessor
	n.tableMut.RUnlock()
	if IsInBetweenLeftExclusive(keyInt, AddrToInt(pred), n.id) {
		n.dataMut.RLock()
		copier.Copy(val, n.data[key])
		n.dataMut.RUnlock()
		return nil
	}
	return errors.New("GetHashTable: Invalid rpcput request")
}

func (n *Node) GetPredecessor(arg int, pred *string) error {
	n.tableMut.RLock()
	*pred = n.Predecessor
	n.tableMut.RUnlock()
	return nil
}

func (n *Node) GetSuccessor(arg int, succ *string) error {
	n.tableMut.RLock()
	*succ = n.Fingers[0].Addr
	n.tableMut.RUnlock()
	return nil
}

// ClosestPrecedingFinger but callable by RPC
func (n *Node) ForeignClosestPrecedingFinger(id *big.Int, res *string) error {
	*res = n.ClosestPrecedingFinger(id)
	return nil
}

// FindSuccessor but callable by RPC
func (n *Node) ForeignFindSuccessor(id *big.Int, res *string) error {
	ret, err := n.FindSuccessor(id)
	if err != nil {
		return err
	}
	*res = ret
	return nil
}

// called by our predecessor to ask if we have keys it should have instead
func (n *Node) Migrate(addr string, dataArr *[]project.DhtData) error {
	keyInt := AddrToInt(addr)
	n.tableMut.RLock()
	pred := n.Predecessor
	n.tableMut.RUnlock()
	if pred != "" && IsInBetweenLeftExclusive(keyInt, AddrToInt(pred), n.id) {
		n.dataMut.Lock()
		for key, element := range n.data {
			keyIntInLoop := AddrToInt(key)
			if IsInBetweenLeftExclusive(keyIntInLoop, AddrToInt(pred), keyInt) {
				*dataArr = append(*dataArr, project.DhtData{Key: key, Val: element})
				delete(n.data, key)
			}
		}
		n.dataMut.Unlock()
	}
	return nil
}

// Return list of fingers
// Not used internally but by external tools to inspect the network
func (n *Node) GetFingers(arg int, fingers *[]string) error {
	n.tableMut.RLock()
	for i := 0; i < NumFingers; i++ {
		*fingers = append(*fingers, n.Fingers[i].Addr)
	}
	n.tableMut.RUnlock()
	return nil
}

// Print information about ourself
func (n *Node) PrintInfo() {
	n.tableMut.RLock()
	fmt.Println("I am", n.addr)
	fmt.Println("PRED", n.Predecessor)
	fmt.Println("SUCC", n.Fingers[0].Addr)
	for i := 0; i < NumSuccessors; i++ {
		fmt.Println("FINGER", i, n.Successors[i])
	}
	n.tableMut.RUnlock()
}

/* === RPC === */

func RPC(addr, method string, args, reply interface{}) error {
	if addr == "" {
		return errors.New("RPC: rpc address was empty")
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		return err
	}
	return nil
}

func RPCNotifyPredecessorJoin(addr string, addrJoin string) error {
	return RPC(addr, "Node.NotifyPredecessorJoin", addrJoin, nil)
}

func RPCPut(addr string, dhtData *project.DhtData) error {
	return RPC(addr, "Node.PutHashTable", dhtData, nil)
}

func RPCGet(addr string, key string) (*project.DhtVal, error) {
	var val project.DhtVal
	return &val, RPC(addr, "Node.GetHashTable", key, &val)
}

func RPCGetSuccessor(addr string) (string, error) {
	var val string
	err := RPC(addr, "Node.GetSuccessor", 0, &val)
	return val, err
}

func RPCGetPredecessor(addr string) (string, error) {
	var val string
	err := RPC(addr, "Node.GetPredecessor", 0, &val)
	return val, err
}

func RPCClosestPrecedingFinger(addr string, id *big.Int) (string, error) {
	var val string
	err := RPC(addr, "Node.ForeignClosestPrecedingFinger", id, &val)
	return val, err
}

func RPCUpdateFingerTable(addr string, up *FingerUpdate) error {
	return RPC(addr, "Node.UpdateFingerTable", up, nil)
}

func RPCFindSuccessor(addr string, id *big.Int) (string, error) {
	var val string
	err := RPC(addr, "Node.ForeignFindSuccessor", id, &val)
	return val, err
}

func RPCMigrate(addr, self string) (*[]project.DhtData, error) {
	dataArr := make([]project.DhtData, 0)
	err := RPC(addr, "Node.Migrate", self, &dataArr)
	if err != nil {
		return nil, err
	}
	return &dataArr, nil
}

func RPCGetFingers(addr string) (*[]string, error) {
	arr := make([]string, 0)
	err := RPC(addr, "Node.GetFingers", 0, &arr)
	if err != nil {
		return nil, err
	}
	return &arr, nil
}
