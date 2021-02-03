package gossip

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.dedis.ch/kyber/v3"

	"github.com/stretchr/testify/require"
)

var factory = GetFactory()

// The subfolders of the temporary folder that stores files
const (
	sharedDataFolder = "shared"
	downloadFolder   = "download"
	chunkSize        = 8192
)

// Test DKG Project
/*
func TestDkg(t *testing.T) {
	numNodes := 3
	th := int(math.Ceil(float64(numNodes/2)) + 1)
	//th := numNodes
	CNodes := make([]string, 0)
	for i := 1; i <= numNodes; i++ {
		CNodes = append(CNodes, "127.0.0.1:"+strconv.Itoa(8080+i))
	}
	nA := createAndStartNode(t, "c", 1, "NA", "", CNodes)
	nB := createAndStartNode(t, "c", 2, "NB", "", CNodes)
	nC := createAndStartNode(t, "c", 3, "NC", "", CNodes)
	//nD := createAndStartNode(t, "c", 4, "ND", "", CNodes)
	//nE := createAndStartNode(t, "c", 5, "NE", "", CNodes)

	defer stopNodes(&nA, &nB, &nC) // &nD, &nE)

	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, // "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()
	/*
		inA := nA.getIns(ctx)
		inB := nB.getIns(ctx)
		inC := nC.getIns(ctx)
		inD := nD.getIns(ctx)
		inE := nE.getIns(ctx)

	nA.gossiper.StartCothority()
	nB.gossiper.StartCothority()
	nC.gossiper.StartCothority()
	//nD.gossiper.StartCothority()
	//nE.gossiper.StartCothority()

	time.Sleep(3 * time.Second)

	//verify reception of public keys (ephimeral)
	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		for _, p := range cn.participants {
			require.True(t, p.pkReceived)
		}
	}

	//cn := nA.gossiper.GetCn()
	//qual := cn.dkg.QUAL()
	//verify the end of the dkg protocol
	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		require.True(t, cn.endDkg)
		require.LessOrEqual(t, th, len(cn.dkg.QualifiedShares()))
		require.LessOrEqual(t, th, len(cn.dkg.QUAL()))
		t.Log("qualified shares:", cn.dkg.QualifiedShares())
		t.Log("QUAL", cn.dkg.QUAL())
	}

	var PublicKey kyber.Point
	cn := nA.gossiper.GetCn()
	PublicKey = cn.publicKey

	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		require.True(t, PublicKey.Equal(cn.publicKey))
		t.Log("secret share: ", cn.priShare)
	}
	t.Log("distributed pub key: ", PublicKey)
}

func TestDkg_More(t *testing.T) {
	numNodes := 5
	th := int(math.Ceil(float64(numNodes/2)) + 1)
	//th := numNodes
	CNodes := make([]string, 0)
	for i := 1; i <= numNodes; i++ {
		CNodes = append(CNodes, "127.0.0.1:"+strconv.Itoa(8080+i))
	}
	nA := createAndStartNode(t, "c", 1, "NA", "", CNodes)
	nB := createAndStartNode(t, "c", 2, "NB", "", CNodes)
	nC := createAndStartNode(t, "c", 3, "NC", "", CNodes)
	nD := createAndStartNode(t, "c", 4, "ND", "", CNodes)
	nE := createAndStartNode(t, "c", 5, "NE", "", CNodes)

	defer stopNodes(&nA, &nB, &nC) // &nD, &nE)

	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()
	/*
		inA := nA.getIns(ctx)
		inB := nB.getIns(ctx)
		inC := nC.getIns(ctx)
		inD := nD.getIns(ctx)
		inE := nE.getIns(ctx)

	nA.gossiper.StartCothority()
	nB.gossiper.StartCothority()
	nC.gossiper.StartCothority()
	nD.gossiper.StartCothority()
	nE.gossiper.StartCothority()

	time.Sleep(2 * time.Second)

	//verify reception of public keys (ephimeral)
	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		for _, p := range cn.participants {
			require.True(t, p.pkReceived)
		}
	}

	//cn := nA.gossiper.GetCn()
	//qual := cn.dkg.QUAL()
	//verify the end of the dkg protocol
	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		require.True(t, cn.endDkg)
		require.LessOrEqual(t, th, len(cn.dkg.QualifiedShares()))
		require.LessOrEqual(t, th, len(cn.dkg.QUAL()))
		t.Log("qualified shares:", cn.dkg.QualifiedShares())
		t.Log("QUAL", cn.dkg.QUAL())
	}

	var PublicKey kyber.Point
	cn := nA.gossiper.GetCn()
	PublicKey = cn.publicKey

	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		require.True(t, PublicKey.Equal(cn.publicKey))
		t.Log("secret share: ", cn.priShare)
	}
	t.Log("distributed pub key: ", PublicKey)
}
*/
func TestDkg_Alot(t *testing.T) {
	numNodes := 9
	th := int(math.Ceil(float64(numNodes/2)) + 1)
	//th := numNodes
	CNodes := make([]string, 0)
	for i := 0; i < numNodes; i++ {
		CNodes = append(CNodes, "127.0.0.1:"+strconv.Itoa(8080+i))
	}
	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		n := createAndStartNode(t, "c", i, string(rune('A'+i)), "", CNodes)
		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()
	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()

	/*
		inA := nA.getIns(ctx)
		inB := nB.getIns(ctx)
		inC := nC.getIns(ctx)
		inD := nD.getIns(ctx)
		inE := nE.getIns(ctx)
	*/
	for _, n := range nodes {
		n.gossiper.StartCothority()
	}
	time.Sleep(time.Duration(numNodes*2) * time.Second)

	//verify reception of public keys (ephimeral)
	finished := 0
	//cn := nA.gossiper.GetCn()
	//qual := cn.dkg.QUAL()
	//verify the end of the dkg protocol
	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		if cn.CheckEndDkg() {
			finished++
		}
	}
	require.GreaterOrEqual(t, finished, th)
	fmt.Println("FINISHED: ", finished)

	var PublicKey kyber.Point
	for i := 0; i < numNodes; i++ {
		cn := nodes[i].gossiper.GetCn()
		if cn.CheckEndDkg() {
			PublicKey = cn.publicKey
			break
		}
	}

	for _, node := range nodes {
		cn := node.gossiper.GetCn()
		if cn.CheckEndDkg() {
			t.Log("secret share: ", cn.priShare)
			require.True(t, PublicKey.Equal(cn.publicKey))
		}
	}
	t.Log("distributed pub key: ", PublicKey)
}

// -----------------------------------------------------------------------------
// Utility functions

type nodeInfo struct {
	addr     string
	id       string
	gossiper BaseGossiper
	folder   string
	stream   chan GossipPacket // optional

	// file created in the working directory that must be deleted
	localFiles []string

	template nodeTemplate
}

func newNodeInfo(g BaseGossiper, addr string, t nodeTemplate) *nodeInfo {
	return &nodeInfo{
		id:       g.GetIdentifier(),
		addr:     addr,
		gossiper: g,
		template: t,
	}
}

func (n nodeInfo) shareDir() string {
	return n.template.sharedDataFolder
}

func (n nodeInfo) downloadDir() string {
	return n.template.downloadFolder
}

func (n nodeInfo) getIns(ctx context.Context) *history {
	watchIn := &history{p: make([]packet, 0)}
	chanIn := n.gossiper.Watch(ctx, true)

	go func() {
		for p := range chanIn {
			watchIn.Lock()
			watchIn.p = append(watchIn.p, packet{p.Addr, p.Msg})
			// fmt.Println(p)
			watchIn.Unlock()
		}
	}()

	return watchIn
}

func (n nodeInfo) getOuts(ctx context.Context) *history {
	watchOut := &history{p: make([]packet, 0)}
	chanOut := n.gossiper.Watch(ctx, false)

	go func() {
		for p := range chanOut {
			watchOut.Lock()
			watchOut.p = append(watchOut.p, packet{p.Addr, p.Msg})
			// fmt.Println(p)
			watchOut.Unlock()
		}
	}()

	return watchOut
}

func (n nodeInfo) getCallbacks(ctx context.Context) *history {
	msgs := &history{p: make([]packet, 0)}

	n.gossiper.RegisterCallback(func(origin string, message GossipPacket) {
		msgs.Lock()
		msgs.p = append(msgs.p, packet{origin, message})
		msgs.Unlock()
	})

	return msgs
}

// addFile adds a file to the current working directory. This is needed because
// the indexFile method looks for the file in that place.
func (n *nodeInfo) addFile(t *testing.T, name string, content []byte) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	err = ioutil.WriteFile(wd+"/"+name, content, os.ModePerm)
	require.NoError(t, err)

	n.localFiles = append(n.localFiles, wd+"/"+name)
}

func (n *nodeInfo) stop() {
	n.gossiper.Stop()

	if n.folder != "" {
		os.RemoveAll(n.folder)
	}

	for _, localFile := range n.localFiles {
		os.Remove(localFile)
	}
}

func createAndStartNode(t *testing.T, role string, cnIndex int, name string, doctorKey string, CNodes []string, opts ...nodeOption) nodeInfo {
	var addr string
	if role == "c" {
		addr = "127.0.0.1:" + strconv.Itoa(8080+cnIndex)
	} else {
		addr = "127.0.0.1:0"
	}
	n := createNode(t, factory, addr, name, role, doctorKey, CNodes, cnIndex, opts...)

	startNodesBlocking(t, n.gossiper)

	n.addr = n.gossiper.GetLocalAddr()

	return n
}

/*
func createAndStartBinNode(t *testing.T, name string, opts ...nodeOption) nodeInfo {

	n := createNode(t, NewBinGossipFactory("./hw3.linux.amd64"), "127.0.0.1:0", name, opts...)

	startNodesBlocking(t, n.gossiper)

	n.addr = n.gossiper.GetLocalAddr()

	return n
}
*/
// createNode creates a node but don't start it. The address of the node is
// unknown until it has been started.
func createNode(t *testing.T, fac GossipFactory, addr, name string, role string, doctorKey string, CNodes []string, cnIndex int, opts ...nodeOption) nodeInfo {

	template := newNodeTemplate().apply(opts...)

	//fullName := fmt.Sprintf("%v---%v", name, t.Name())
	fullName := name
	var folder string
	var err error

	if template.sharedDataFolder == "" || template.downloadFolder == "" {
		folder, err = ioutil.TempDir("", "gossiper-"+name+"-")
		require.NoError(t, err)

		if template.sharedDataFolder == "" {
			err = os.Mkdir(folder+"/"+sharedDataFolder, os.ModePerm)
			require.NoError(t, err)

			_, err = os.Create(folder + "/" + sharedDataFolder + "/indexedState.json")
			require.NoError(t, err)

			template.sharedDataFolder = folder + "/" + sharedDataFolder
		}

		if template.downloadFolder == "" {
			err = os.Mkdir(folder+"/"+downloadFolder, os.ModePerm)
			require.NoError(t, err)

			template.downloadFolder = folder + "/" + downloadFolder
		}
	}

	node, err := fac.New(addr, fullName, template.sharedDataFolder, template.downloadFolder, role, doctorKey, CNodes, CNodes[(cnIndex+1)%len(CNodes)], CNodes[(cnIndex+1)%len(CNodes)])
	require.NoError(t, err)

	require.Equal(t, fullName, node.GetIdentifier())

	return nodeInfo{
		id:       node.GetIdentifier(),
		gossiper: node,
		folder:   folder,
		template: *template,
	}
}

type nodeTemplate struct {
	antiEntropy int
	routeTimer  int

	sharedDataFolder string
	downloadFolder   string

	numParticipants int
	nodeIndex       int
	paxosRetry      int
}

func newNodeTemplate() *nodeTemplate {
	return &nodeTemplate{
		antiEntropy: 1000,
		routeTimer:  0,

		nodeIndex:       0,
		numParticipants: 1,
		paxosRetry:      1000,
	}
}

func (t *nodeTemplate) apply(opts ...nodeOption) *nodeTemplate {
	for _, opt := range opts {
		opt(t)
	}

	return t
}

type nodeOption func(*nodeTemplate)

func WithAntiEntropy(value int) nodeOption {
	return func(n *nodeTemplate) {
		n.antiEntropy = value
	}
}

func WithRouteTimer(value int) nodeOption {
	return func(n *nodeTemplate) {
		n.routeTimer = value
	}
}

func WithSharedDataFolder(folder string) nodeOption {
	return func(n *nodeTemplate) {
		n.sharedDataFolder = folder
	}
}

func WithDownloadFolder(folder string) nodeOption {
	return func(n *nodeTemplate) {
		n.downloadFolder = folder
	}
}

func WithNumParticipants(num int) nodeOption {
	return func(n *nodeTemplate) {
		n.numParticipants = num
	}
}

func WithNodeIndex(index int) nodeOption {
	return func(n *nodeTemplate) {
		n.nodeIndex = index
	}
}

func WithPaxosRetry(d int) nodeOption {
	return func(n *nodeTemplate) {
		n.paxosRetry = d
	}
}

// startNodesBlocking waits until the node is started
func startNodesBlocking(t *testing.T, nodes ...BaseGossiper) {
	wg := new(sync.WaitGroup)
	wg.Add(len(nodes))
	for idx := range nodes {
		go func(i int) {
			defer wg.Done()
			ready := make(chan struct{})
			go nodes[i].Run(ready)
			<-ready
		}(idx)
	}
	wg.Wait()
}

type packet struct {
	origin  string
	message GossipPacket
}

type history struct {
	sync.Mutex
	p []packet
}

func (h *history) getPs() []packet {
	h.Lock()
	defer h.Unlock()

	return append([]packet{}, h.p...)
}

type linkBuilder struct {
	nodes map[string]nodeInfo
}

func newLinkBuilder(nodes map[string]nodeInfo) linkBuilder {
	return linkBuilder{nodes}
}

func (b linkBuilder) add(link string) {
	// we accept the following links:
	// "A --> B"
	// "A <-- B"
	// "A <-> B"

	parts := strings.Split(link, " ")
	nA := b.nodes[parts[0]]
	nB := b.nodes[parts[2]]

	switch parts[1] {
	case "-->":
		nA.gossiper.AddRoute(nB.id, nB.addr)
		nA.gossiper.AddAddresses(nB.addr)
	case "<--":
		nB.gossiper.AddRoute(nA.id, nA.addr)
		nB.gossiper.AddAddresses(nA.addr)
	case "<->":
		nA.gossiper.AddRoute(nB.id, nB.addr)
		nB.gossiper.AddRoute(nA.id, nA.addr)

		nA.gossiper.AddAddresses(nB.addr)
		nB.gossiper.AddAddresses(nA.addr)
	default:
		fmt.Println("[WARNING] unknown link", parts[1])
	}
}

func (b linkBuilder) connectAll() {
	for idFrom := range b.nodes {
		for idTo := range b.nodes {
			if idTo == idFrom {
				continue
			}

			b.add(idFrom + " --> " + idTo)
		}
	}
}

// stopNodes stops a set of nodes concurrently
func stopNodes(nodes ...*nodeInfo) {
	wg := sync.WaitGroup{}
	wg.Add(len(nodes))

	for _, node := range nodes {
		go func(n *nodeInfo) {
			n.stop()
			wg.Done()
		}(node)
	}

	wg.Wait()
}
