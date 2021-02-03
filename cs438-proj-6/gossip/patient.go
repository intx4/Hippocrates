package gossip

//Expose the Patient API

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/project"

	"go.dedis.ch/kyber/v3"
)

const chunk_size = 8192

type Patient struct {
	cothorityAddr []string

	pkMux       sync.RWMutex
	pkSet       bool
	pkChan      chan bool
	cothorityPk kyber.Point //public key of Cothority

	filesMux       sync.RWMutex
	filesPositions map[string]*AuthInfo //map: filename->Information for cothority. Maybe we don't need this

	g *Gossiper
}

//Wrapper for sending the information of where to retrieve a file to the cothority
type AuthInfo struct {
	FileName  string `json:"FileName"`
	Owner     string `json:"owner"`
	TotChunks int    `json:"totchunks"` //total len of file in chunks
	Copies    int    `json:"copies"`    //number of copies for chunk for this file. Ex 3 means each chunk has 3 copies
	AesKey    []byte `json:"aeskey"`    // key for decrypting file
	//Locations not needed
	//Locations map[int][]*Location `json:"location"`  //map: chunk_index->location info
}

//Wrapper for the location info in the cothority
type Location struct {
	CopyNum  int    `json:"copynum"`  //number of the copy for this chunk
	Addr     string `json:"addr"`     //address of the node holding it in chord
	KeyChord string `json:"keychord"` //hex of SHA1(OwnerID||FileName||chunk_index||copy_number) the doctor should be able to compute this
}

func NewPatient(g *Gossiper, cothorityAddr []string) *Patient {
	addresses := make([]string, 0)
	for _, addr := range cothorityAddr {
		addresses = append(addresses, addr)
	}
	return &Patient{
		pkSet:         false,
		cothorityAddr: addresses,
		g:             g,
	}
}

func (p *Patient) PublishFile(filePath string, shareDir string) error {

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		Log(err)
	}
	stat, err := os.Stat(filePath)
	if err != nil {
		Log(err)
	}

	//copy the file into shared folder
	ioutil.WriteFile(shareDir+"/"+stat.Name(), data, os.ModePerm)

	//split file into chunks
	chunks := SplitFile(data)

	//generate key for AES
	key := make([]byte, 24) //AES-192
	_, err = rand.Read(key) //now key should contain rand bytes
	if err != nil {
		Log(err)
		return err
	}

	enChunks, ivList := EncryptChunks(chunks, key)

	//distribute chunks into chord. The key is SHA1(gossiper.ID||stat.Name()||indexChunk||copyNumber)
	//Use DhtData, key and DHtVal as wrappers in project_packets

	//TO DO: form the AuthInfo struct and after putting in the dht call the sendEncFile of patient
	userGossiperID := p.g.GetIdentifier()
	entryHasher := sha1.New()
	entryHasher.Write([]byte(userGossiperID + stat.Name()))
	entryHashBytes := entryHasher.Sum(nil)
	entryHash := hex.EncodeToString(entryHashBytes)

	for copyNumber := 0; copyNumber < 5; copyNumber++ {
		for i, enchunk := range enChunks {
			h := sha1.New()
			dhtkeyBytes := []byte(userGossiperID + stat.Name() + strconv.Itoa(i) + strconv.Itoa(copyNumber))
			h.Write(dhtkeyBytes)
			dhtKey := hex.EncodeToString(h.Sum(nil))
			dhtVal := project.DhtVal{Index: i, EntryHash: entryHash, EnChunk: enchunk, Iv: ivList[i]}
			p.g.DhtNode.Put(dhtKey, &dhtVal)
		}
	}
	//Form AuthInfo struct and then encode its json representation with the same key used for file
	//fmt.Println("Here")
	info := &AuthInfo{FileName: stat.Name(), Owner: userGossiperID, TotChunks: len(enChunks), Copies: 5, AesKey: key}
	message, err := json.Marshal(info)
	if err != nil {
		Log(err)
		return err
	}
	//fmt.Println("encFileInfo")
	encFileInfo, iv := EncryptInfo(message, key)

	//Send to cothority
	//fmt.Println("Sending stuff to cn")
	p.SendEncFile(key, iv, encFileInfo, entryHash)

	//fmt.Println("Encrypting file with key: ", key)
	return nil
}

//TEST
/*
func (p *Patient) PublishMock(what string) {
	authInfo := &AuthInfo{FileName: what, Owner: p.g.GetIdentifier(), TotChunks: 0}
	p.SendEncFile(authInfo)
}
*/
//Periodically sends request for cothority pk until he receives it
func (p *Patient) RequestPk() {
	pkt := new(GossipPacket)
	pkt.Hippo = new(project.HippoMsg)
	pkt.Hippo.PkReq = &project.PkRequest{Req: true}
	ticker := time.NewTicker(1 * time.Second)
	//time.Sleep(1 * time.Second) //give time to cothority to set up
	p.g.sendToRandomAmong(*pkt, p.cothorityAddr)
	select {
	case <-ticker.C:
		p.g.sendToRandomAmong(*pkt, p.cothorityAddr)
	case <-p.pkChan:
		ticker.Stop()
		return
	}
}
func (p *Patient) CheckPk() bool {
	p.pkMux.RLock()
	defer p.pkMux.RUnlock()
	return p.pkSet
}

//Set public key
func (p *Patient) SetPk(pubkey kyber.Point) {
	p.pkMux.Lock()
	if !p.pkSet {
		p.cothorityPk = pubkey
		p.pkSet = true
		p.pkMux.Unlock()
		go func() {
			p.pkChan <- true
		}()
		return
	}
	p.pkMux.Unlock()
}

// Message encryption:(schema from kyber)
//
// r: random point
// A: dkg public key
// G: curve's generator
// M: message to encrypt
// (C, U): encrypted message
//
// C = rA + M
// U = rG
//
// Encrypt AES key with cothority pk
func (p *Patient) SendEncFile(key []byte, iv []byte, encInfo []byte, entryHash string) {
	if !p.CheckPk() {
		go p.RequestPk()
		for !p.CheckPk() {
			time.Sleep(1 * time.Second)
		}
	}
	A := p.cothorityPk
	r := suite.Scalar().Pick(suite.RandomStream())
	M := suite.Point().Embed(key, suite.RandomStream())
	C := suite.Point().Add( // rA + M
		suite.Point().Mul(r, A), // rA
		M,
	)
	U := suite.Point().Mul(r, nil) // rG

	var pkt GossipPacket
	pkt.Hippo = new(project.HippoMsg)
	c, err := C.MarshalBinary()
	if err != nil {
		Log(err)
		return
	}
	u, err := U.MarshalBinary()
	if err != nil {
		Log(err)
		return
	}
	//fmt.Println("Patient", "c", "u", c, u)
	points := &project.Points{C: c, U: u}
	pkt.Hippo.EncFileI = &project.EncFileInfo{Points: points, Iv: iv, EntryHash: entryHash, EncInfo: encInfo}
	p.g.BroadcastMessageAmong(pkt, p.cothorityAddr)
}

//UTILITIES------------------------------------------------------------------------------------------------
func Log(err error) {
	fmt.Println("Error: ", err)
}
func SplitFile(data []byte) [][]byte {
	chunks := make([][]byte, 0)
	buf := make([]byte, 0)
	for i, b := range data {
		buf = append(buf, b)
		if (i+1)%chunk_size == 0 {
			chunks = append(chunks, buf[:])
			buf = make([]byte, 0)
		}
	}
	if len(data)%chunk_size != 0 {
		//final dump
		chunks = append(chunks, buf[:])
	}
	return chunks
}

//AES-192
func EncryptChunks(chunks [][]byte, key []byte) ([][]byte, [][]byte) {
	c, err := aes.NewCipher(key)
	if err != nil {
		Log(err)
		return nil, nil
	}

	enChunks := make([][]byte, 0)
	ivList := make([][]byte, 0)

	for _, chunk := range chunks {
		iv := make([]byte, c.BlockSize())
		_, err = rand.Read(iv) //random IV

		cbc := cipher.NewCBCEncrypter(c, iv)
		plain, err := pkcs7Pad(chunk, c.BlockSize()) //pad chunk
		if err != nil {
			Log(err)
			return nil, nil
		}

		enChunk := make([]byte, len(plain))
		cbc.CryptBlocks(enChunk, plain)

		enChunks = append(enChunks, enChunk)
		ivList = append(ivList, iv)
	}
	return enChunks, ivList
}

//AES-192
func EncryptInfo(info, key []byte) ([]byte, []byte) {
	c, err := aes.NewCipher(key)
	if err != nil {
		Log(err)
		return nil, nil
	}
	iv := make([]byte, c.BlockSize())
	_, err = rand.Read(iv) //random IV
	//fmt.Println("Encrypting info with iv", iv)
	cbc := cipher.NewCBCEncrypter(c, iv)
	plain, err := pkcs7Pad(info, c.BlockSize()) //pad chunk
	if err != nil {
		Log(err)
		return nil, nil
	}
	cipher := make([]byte, len(plain))
	cbc.CryptBlocks(cipher, plain)

	return cipher, iv
}

//From https://gist.github.com/huyinghuan/7bf174017bf54efb91ece04a48589b22
var (
	// ErrInvalidBlockSize indicates hash blocksize <= 0.
	ErrInvalidBlockSize = errors.New("invalid blocksize")

	// ErrInvalidPKCS7Data indicates bad input to PKCS7 pad or unpad.
	ErrInvalidPKCS7Data = errors.New("invalid PKCS7 data (empty or not padded)")

	// ErrInvalidPKCS7Padding indicates PKCS7 unpad fails to bad input.
	ErrInvalidPKCS7Padding = errors.New("invalid padding on input")
)

func pkcs7Pad(b []byte, blocksize int) ([]byte, error) {
	if blocksize <= 0 {
		return nil, ErrInvalidBlockSize
	}
	if b == nil || len(b) == 0 {
		return nil, ErrInvalidPKCS7Data
	}
	n := blocksize - (len(b) % blocksize)
	pb := make([]byte, len(b)+n)
	copy(pb, b)
	copy(pb[len(b):], bytes.Repeat([]byte{byte(n)}, n))
	return pb, nil
}
