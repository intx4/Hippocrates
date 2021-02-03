package gossip

//Expose the cothority nodes API

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/project"
	"go.dedis.ch/cs438/hw3/gossip/types"
	"go.dedis.ch/kyber/v3"
	"go.dedis.ch/kyber/v3/group/edwards25519"
	"go.dedis.ch/kyber/v3/share"
	dkg "go.dedis.ch/kyber/v3/share/dkg/pedersen"
)

var suite = edwards25519.NewBlakeSHA256Ed25519()

type CothorityNode struct {
	//DKG PART
	//from kyber
	dkgMux  sync.RWMutex
	dkg     *dkg.DistKeyGenerator
	pubKey  kyber.Point  // ephimeral pub key for dkg setup
	privKey kyber.Scalar // ephimeral pr key

	dealsMux sync.RWMutex
	deals    []*dkg.Deal

	respsMux sync.RWMutex
	resps    []*dkg.Response

	//custom
	index        uint32
	partMux      sync.RWMutex
	participants []*Participant //list of all cothority nodes at tuple (pubKey,address). Index are consistent among nodes
	g            *Gossiper      //pointer to Gossiper. Note that the logic is Gossiper->Cothority

	endDkgMux sync.RWMutex
	endDkg    bool            //to flag if protocol ended
	timer     time.Time       //for testing
	priShare  *share.PriShare //secret share at the end of protocol
	publicKey kyber.Point     //public Key of cothority at the end of protocol
	//END OF DKG

	patientAPI *PatientAPI //Interface for dealing with patients
	doctorAPI  *DoctorAPI  //interface for dealing with doctors
}

type Participant struct {
	pubKey     kyber.Point
	address    string
	pkReceived bool
}

type PatientAPI struct {
	storageMux sync.RWMutex
	//Need to change this with mapping to C and U that are the encrypted info
	fileStorage map[string]*Efi //map: SHA1(OwnerID||Filename) -> Information
}
type Efi struct {
	points  *project.Points
	encInfo []byte
	iv      []byte
}
type DoctorAPI struct {
	doctorMux sync.RWMutex
	doctorsDb map[string]string //map: DoctorID -> secret key for HMAC
}

func NewCothorityNode(g *Gossiper, addresses []string, doctorCsv string) *CothorityNode {
	myAddr := g.addrStr
	privKey := suite.Scalar().Pick(suite.RandomStream())
	pubKey := suite.Point().Mul(privKey, nil)
	participants := make([]*Participant, 0)
	var index uint32
	for i := 0; i < len(addresses); i++ {
		p := new(Participant)
		p.address = addresses[i]
		p.pkReceived = false
		p.pubKey = suite.Point().Null()
		if addresses[i] == myAddr {
			index = uint32(i)
			p.pubKey = pubKey
			p.pkReceived = true
		}
		participants = append(participants, p)
	}
	pA := new(PatientAPI)
	dA := new(DoctorAPI)

	pA.fileStorage = make(map[string]*Efi)
	dA.doctorsDb = PopulateDoctorDb(doctorCsv)

	return &CothorityNode{
		pubKey:       pubKey,
		privKey:      privKey,
		deals:        make([]*dkg.Deal, 0),
		resps:        make([]*dkg.Response, 0),
		participants: participants,
		index:        index,
		endDkg:       false,
		patientAPI:   pA,
		doctorAPI:    dA,
		g:            g,
	}
}

//DKG PROTOCOL--------------------------------------------------------------------------------------------

//Node will broadcast among the cothority its public key. To be done at startup
func (cn *CothorityNode) DeliverPubKey() {

	pk, _ := cn.pubKey.MarshalBinary()
	pubKeyMsg := &project.PkMessage{Index: cn.index, PubKey: pk}
	pkt := cn.g.vc.generateNewRumor_Dkg(cn.g.GetIdentifier(), "", pubKeyMsg, nil)
	nodes := make([]string, 0) //cothority nodes
	for _, p := range cn.participants {
		if p.address != cn.g.addrStr {
			nodes = append(nodes, p.address)
		}
	}
	//cn.g.BroadcastMessageAmong(pkt, nodes)
	cn.g.sendToRandomAmong(pkt, nodes)
	cn.timer = time.Now()

	fmt.Println(cn.g.GetIdentifier(), " :Cothority started")
}

//Store pubkey of participant index
func (cn *CothorityNode) StorePubKey(index uint32, pubKey []byte) {
	cn.partMux.Lock()
	defer cn.partMux.Unlock()

	cn.participants[index].pubKey.UnmarshalBinary(pubKey)
	cn.participants[index].pkReceived = true

	//Check if we have all the publicKeys, if so init the dkg struct
	initDkg := true
	pubKeys := make([]kyber.Point, 0)

	for _, p := range cn.participants {
		if !p.pkReceived {
			initDkg = false
			break
		} else {
			pubKeys = append(pubKeys, p.pubKey)
		}
	}

	if initDkg {
		go cn.InitDkg(cn.privKey, pubKeys, len(cn.participants))
	}
}

//Init dkg and send deals
func (cn *CothorityNode) InitDkg(sK kyber.Scalar, pKs []kyber.Point, n int) {
	cn.dkgMux.Lock()
	defer cn.dkgMux.Unlock()

	t := int(math.Ceil(float64(n/2)) + 1)
	//t = n
	dkg, err := dkg.NewDistKeyGenerator(suite, sK, pKs, t)
	if err == nil {
		cn.dkg = dkg
	} else {
		Log(err)
	}
	//go cn.ServeNodes()
	//generate and send deals
	deals, err := cn.dkg.Deals()
	if err == nil {
		go cn.DeliverDeals(deals)
	} else {
		Log(err)
	}
}

//Creates Deals. Deliver them
func (cn *CothorityNode) DeliverDeals(deals map[int]*dkg.Deal) {
	cn.partMux.RLock()
	defer cn.partMux.RUnlock()
	//send deal i to participant i
	//Let's try to synchronize a little bit
	time.Sleep(time.Duration((cn.index+1)*1000) * time.Millisecond)
	for i, deal := range deals {
		dkgMsg := new(project.DkgMessage)
		dkgMsg.DealMsg = &project.DealMessage{To: uint32(i), Deal: deal}
		var pkt GossipPacket
		pkt.Hippo = &project.HippoMsg{DkgMsg: dkgMsg}
		if i != int(cn.index) {
			//own deal already processed in Deals. Don't worry about it
			go cn.g.sendPacket(pkt, cn.participants[i].address)
		}
	}
	//verifies after timeout is protocol ended
	go cn.VerifyCertified()
}

//Process an incoming Deal
func (cn *CothorityNode) ProcessDeal(deal *dkg.Deal) {
	//Store deal
	cn.dealsMux.Lock()
	cn.deals = append(cn.deals, deal)
	cn.dealsMux.Unlock()

	//Process deal

	resp := new(dkg.Response)
	ok := false
	var err error
	//maybe wait for public key
	for !ok {
		cn.dkgMux.Lock()
		if cn.dkg != nil {
			resp, err = cn.dkg.ProcessDeal(deal)
			ok = true
		}
		cn.dkgMux.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	if err == nil {
		cn.DeliverResponse(resp)
	} else {
		Log(err)
	}

}

//Gossip a Response
func (cn *CothorityNode) DeliverResponse(resp *dkg.Response) {
	response := &project.DealResponse{Resp: resp, From: cn.index}
	pkt := cn.g.vc.generateNewRumor_Dkg(cn.g.GetIdentifier(), "", nil, response)
	nodes := make([]string, 0) //cothority nodes
	for j, p := range cn.participants {
		if j != int(cn.index) {
			//do not send response to myself
			nodes = append(nodes, p.address)
		}
	}
	//cn.g.BroadcastMessageAmong(pkt, nodes)
	cn.g.sendToRandomAmong(pkt, nodes)
}

//Process incoming response
func (cn *CothorityNode) ProcessResponse(resp *dkg.Response, from uint32) {
	//store response
	cn.respsMux.Lock()
	cn.resps = append(cn.resps, resp)
	cn.respsMux.Unlock()

	//process response
	var err error
	ok := false
	for !ok {
		cn.dkgMux.Lock()
		if cn.dkg != nil {
			_, err = cn.dkg.ProcessResponse(resp)
		}
		cn.dkgMux.Unlock()
		if err == nil {
			//Log(err)
			ok = true
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

//It checks whether we ended the dkg protocol after a timeout
func (cn *CothorityNode) VerifyCertified() {
	cap := len(cn.participants)
	eps := 1
	if cap > 10 {
		eps = 2
	}
	if cap > 15 {
		eps = 10
	}
	if cap > 20 {
		eps = 15
	}
	timeout := time.Duration(cap*eps) * time.Second
	ticker := time.NewTicker(timeout)
	t := false
	for !t {
		select {
		case <-ticker.C:
			cn.dkgMux.Lock()
			t := cn.dkg.ThresholdCertified()
			//t := cn.dkg.Certified()
			if t {
				cn.dkg.SetTimeout()
				distrKey, _ := cn.dkg.DistKeyShare()
				cn.priShare = distrKey.PriShare()
				cn.publicKey = distrKey.Public()
				ticker.Stop()
				fmt.Println("Public key: ", cn.publicKey)
				fmt.Println("QUAL:", cn.dkg.QualifiedShares())
				go cn.EndDkg()
			} else {
				timeout = 1 * time.Second
				ticker = time.NewTicker(timeout)
			}
			cn.dkgMux.Unlock()
		}
	}
}

func (cn *CothorityNode) EndDkg() {
	cn.endDkgMux.Lock()
	defer cn.endDkgMux.Unlock()
	cn.endDkg = true
	time := time.Now()
	fmt.Printf("Dkg-end, node %s at time = %d\n", cn.g.GetIdentifier(), int(time.Sub(cn.timer).Seconds()))
	return
}
func (cn *CothorityNode) CheckEndDkg() bool {
	cn.endDkgMux.RLock()
	defer cn.endDkgMux.RUnlock()
	return cn.endDkg
}

//PATIENT API--------------------------------------------------------------------------------------

func (pA *PatientAPI) AcceptFile(entryHash string, encFileInfo *project.EncFileInfo) {
	pA.storageMux.Lock()
	defer pA.storageMux.Unlock()
	pA.fileStorage[entryHash] = &Efi{points: encFileInfo.Points, iv: encFileInfo.Iv, encInfo: encFileInfo.EncInfo}
}

func (pA *PatientAPI) GetEncFileInfo(entryHash string) (*Efi, bool) {
	pA.storageMux.RLock()
	defer pA.storageMux.RUnlock()
	efi, found := pA.fileStorage[entryHash]
	return efi, found
}

//DOCTOR API---------------------------------------------------------------------------------------

func (dA *DoctorAPI) ProcessFileRequest(fr *project.FR, Hmac string, addr string, cn *CothorityNode, g *Gossiper) error {
	if ok := dA.VerifyHmac(fr, Hmac); ok {
		docId := fr.DoctorId
		docPubKey := suite.Point()
		err := docPubKey.UnmarshalBinary(fr.DPubKey)
		if err != nil {
			Log(err)
			return err
		}
		docAddr := addr
		entryHash := fr.EntryHash

		docPubKeyBytes := fr.DPubKey
		fileaccess := types.FileAccess{
			User:    docId,
			Pubkey:  docPubKeyBytes,
			Address: docAddr,
			Time:    time.Now().Unix(), //don't use this. For reliability doctors will send a request to every node
			File:    entryHash,
		}

		acceptedFileAccess := types.FileAccess{}
		prevLen := g.bc.GetLength()
		lastBlockHashBytes := make([]byte, 32)
		//retry if the acceptedBlock do not clash with my proposal
		for !CompareAccess(acceptedFileAccess, fileaccess) {
			lastBlockHash, _ := g.bc.GetLastBlock()
			if lastBlockHash != "" {
				lastBlockHashBytes, _ = hex.DecodeString(lastBlockHash)
			}
			len := g.bc.GetLength()
			//start Paxos algo
			go g.PaxosRun(&fileaccess, len, lastBlockHashBytes)

			for len == prevLen {
				time.Sleep(500 * time.Millisecond)
				len = g.bc.GetLength()
			}
			prevLen = len
			_, last := g.bc.GetLastBlock()
			acceptedFileAccess = last.Access

			//Collect Data and Send Partial Decryption. All in one
			go cn.RetrievePartialDec(acceptedFileAccess, dA)
		}
		return nil
	} else {
		return errors.New("Wrong Hmac")
	}
}

func (dA *DoctorAPI) VerifyHmac(fr *project.FR, Hmac string) bool {
	dA.doctorMux.RLock()
	defer dA.doctorMux.RUnlock()

	key, found := dA.doctorsDb[fr.DoctorId]
	if !found {
		Log(errors.New("Not a Doctor"))
		return false
	}

	H := hmac.New(sha256.New, []byte(key))
	b, err := json.Marshal(fr)
	if err != nil {
		Log(err)
		return false
	}
	H.Write(b)

	computedHmac := H.Sum(nil)
	receivedHmac, err := base64.StdEncoding.DecodeString(Hmac)

	if err != nil {
		Log(err)
		return false
	}

	return hmac.Equal(computedHmac, receivedHmac)
}

//Given a fileAccess, collects data and send Partial Dec
func (cn *CothorityNode) RetrievePartialDec(fA types.FileAccess, dA *DoctorAPI) error {
	entryHash := fA.File
	efi, found := cn.patientAPI.GetEncFileInfo(entryHash)
	if !found {
		Log(errors.New("File not found"))
		return nil
	}
	docPubKey := suite.Point()
	err := docPubKey.UnmarshalBinary(fA.Pubkey)
	if err != nil {
		Log(err)
		return err
	}
	docAddr := fA.Address

	A := cn.publicKey
	priShare := cn.priShare

	dA.SendPartialDec(entryHash, docPubKey, docAddr, efi, priShare, cn.index, A, cn.g)
	return nil
}

//Called by RetrievePartialDec
func (dA *DoctorAPI) SendPartialDec(entryHash string, docPubKey kyber.Point, docAddr string, efi *Efi, priShare *share.PriShare, i uint32, A kyber.Point, g *Gossiper) {
	points := efi.points
	c := points.C
	u := points.U
	C := suite.Point()
	U := suite.Point()
	err := C.UnmarshalBinary(c)
	if err != nil {
		Log(err)
		return
	}
	err = U.UnmarshalBinary(u)
	if err != nil {
		Log(err)
		return
	}
	Q := docPubKey
	V := suite.Point().Add( // oU + oQ
		suite.Point().Mul(priShare.V, U), // oU
		suite.Point().Mul(priShare.V, Q), // oQ
	)
	Vb, err := V.MarshalBinary()
	if err != nil {
		Log(err)
		return
	}
	var pkt GossipPacket
	pkt.Hippo = new(project.HippoMsg)
	a, _ := A.MarshalBinary() //cn pub key
	pkt.Hippo.PartDec = &project.PartialDecrypt{EntryHash: entryHash, C: c, PubShareI: int(i), PubShareV: Vb, A: a, Iv: efi.iv, EncInfo: efi.encInfo}

	g.sendPacket(pkt, docAddr)
}

//UTILITIES---------------------------------------------------------------------------------------------
func (cn *CothorityNode) ReturnParticipantsList() []string {
	cn.partMux.RLock()
	defer cn.partMux.RUnlock()
	nodes := make([]string, 0) //cothority nodes
	for _, p := range cn.participants {
		nodes = append(nodes, p.address)
	}
	return nodes
}

func (cn *CothorityNode) GetPublicKey() (kyber.Point, bool) {
	var point kyber.Point
	if cn.CheckEndDkg() {
		return cn.publicKey, true
	} else {
		return point, false
	}
}

func PopulateDoctorDb(doctorCsv string) map[string]string {
	//Reads the csv file containing doctor secret keys and populates db
	docCsv, err := os.Open(doctorCsv)
	docDb := make(map[string]string)
	if err != nil {
		Log(err)
		return nil
	}
	r := csv.NewReader(docCsv)
	for {
		//Iterate until end of file
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			Log(err)
			return nil
		}
		docDb[record[0]] = record[1]
	}
	return docDb
}

func (cn *CothorityNode) AcceptFile(entryHash string, encFileInfo *project.EncFileInfo) {
	cn.patientAPI.AcceptFile(entryHash, encFileInfo)
}
func (cn *CothorityNode) ProcessFileRequest(fr *project.FR, Hmac string, addr string) {
	cn.doctorAPI.ProcessFileRequest(fr, Hmac, addr, cn, cn.g)
}
func (cn *CothorityNode) SendPartialDec(docPubKey kyber.Point, docAddr string, entryHash string) {
	points, found := cn.patientAPI.GetEncFileInfo(entryHash)
	if found {
		cn.doctorAPI.SendPartialDec(entryHash, docPubKey, docAddr, points, cn.priShare, cn.index, cn.publicKey, cn.g)
	}
}

/*
TO DO:
1-receive a request->chech HMAC->if ok start paxos
->if paxos succeed put in the blockchain -> check last block of blockchain (contains a request)->
if it's in the blockchain it's a valid request -> send your partial decryption to the doctor
ALL DONE, JUST BLOCKCHAIN TO DO
2 - Define new Aes encryption for (key at startup?) new paxos algorithm
*/
//Utilities-----------------------------------------------------------------------------------
func CompareAccess(A, B types.FileAccess) bool {
	return A.File == B.File && A.User == B.User && A.Address == B.Address
}
