package gossip

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"strconv"
	"sync"

	"go.dedis.ch/kyber/v3/share"

	"go.dedis.ch/cs438/hw3/gossip/project"

	"go.dedis.ch/kyber/v3"
)

type Doctor struct {
	cothorityAddr []string

	secret []byte //secret key for HMAC

	Q       kyber.Point  //doctor's pub key for receiving the partial decryptions
	privKey kyber.Scalar //doctor priv key for decryption of partial decryptions
	g       *Gossiper

	partialStorageMux sync.RWMutex
	partialDecStorage map[string]*DecInfo //map: SHA1(Owner||Filename) -> Dec Info

	fileStorageMux sync.RWMutex
	fileStorage    map[string]*ChunkMap //map: SHA1(Owner||FileName) -> storage for chunks
}

type DecInfo struct {
	C         kyber.Point       // C in encrypted file info
	A         kyber.Point       // pub key of cothority
	shares    []*share.PubShare // shares[share.I] = share{I,V} from cothority node I
	counter   int               //how many shares?
	decrypted bool              //key recovered
	iv        []byte            //iv for AES
	encInfo   []byte            //encrypted Info->json of AuthInfo
}

type ChunkMap struct {
	chunks    map[int][]byte //index of chunk -> chunk(decrypted)
	recovered []bool         //how many chunks I recovered
	info      *AuthInfo      //information about this file (retrieved from cothority)
}

func NewDoctor(g *Gossiper, secret string, cothorityAddr []string) *Doctor {
	bsecret := []byte(secret)
	p := suite.Scalar().Pick(suite.RandomStream())
	Q := suite.Point().Mul(p, nil)
	return &Doctor{
		cothorityAddr:     cothorityAddr,
		secret:            bsecret,
		Q:                 Q,
		privKey:           p,
		g:                 g,
		partialDecStorage: make(map[string]*DecInfo),
		fileStorage:       make(map[string]*ChunkMap),
	}
}

//Create a request for a new file given owner's id and filename
func (d *Doctor) RequestFile(user, fileName string) error {
	H := sha1.New()
	s := user + fileName
	H.Write([]byte(s))
	h := H.Sum(nil)
	entryHash := hex.EncodeToString(h)
	hmac, fr := SignMessage(d.secret, d.Q, entryHash, d.g.GetIdentifier())
	if hmac == "" || fr == nil {
		return errors.New("Hmac fail")
	}
	fileReq := &project.FileRequest{Fr: fr, Hmac: hmac}
	var pkt GossipPacket
	pkt.Hippo = &project.HippoMsg{FileReq: fileReq}
	d.g.BroadcastMessageAmong(pkt, d.cothorityAddr)
	return nil
}

func (d *Doctor) CollectPartialDec(entryHash string, pubShare *share.PubShare, C, A kyber.Point, iv []byte, encInfo []byte) {
	d.partialStorageMux.Lock()
	defer d.partialStorageMux.Unlock()

	_, found := d.partialDecStorage[entryHash]
	if !found {
		d.partialDecStorage[entryHash] = &DecInfo{A: A, C: C, counter: 0, decrypted: false, shares: make([]*share.PubShare, len(d.cothorityAddr)), iv: iv, encInfo: encInfo}
	}
	shares := d.partialDecStorage[entryHash].shares
	shares[pubShare.I] = pubShare
	counter := d.partialDecStorage[entryHash].counter
	counter = counter + 1
	decrypted := d.partialDecStorage[entryHash].decrypted

	d.partialDecStorage[entryHash] = &DecInfo{A: A, C: C, counter: counter, decrypted: decrypted, shares: shares, iv: iv, encInfo: encInfo}

	t := int(math.Ceil(float64(len(shares)/2)) + 1)
	//t := len(shares)
	if counter >= t && !decrypted {
		authInfo := DecryptPartials(d.partialDecStorage[entryHash], d.privKey)
		if authInfo == nil {
			Log(errors.New("Failed to decrypt partials"))
			return
		}
		d.partialDecStorage[entryHash] = &DecInfo{A: A, C: C, counter: counter, decrypted: true, shares: shares, iv: iv, encInfo: encInfo}
		go d.RecoverFile(authInfo, d.g.rootDownloadedFiles)
	}
}

func (d *Doctor) RecoverFile(info *AuthInfo, rootDownloadedFiles string) {
	d.fileStorageMux.Lock()

	owner := info.Owner
	fileName := info.FileName
	H := sha1.New()
	s := owner + fileName
	H.Write([]byte(s))
	entry := hex.EncodeToString(H.Sum(nil))

	recovered := make([]bool, info.TotChunks)
	for i, _ := range recovered {
		recovered[i] = false
	}
	d.fileStorage[entry] = &ChunkMap{chunks: make(map[int][]byte), recovered: recovered, info: info}
	d.fileStorageMux.Unlock()

	breakCounter := 0
	for i := 0; i < info.TotChunks; i++ {
		for copyNumber := 0; copyNumber < info.Copies; copyNumber++ {
			h := sha1.New()
			dhtkeyBytes := []byte(info.Owner + info.FileName + strconv.Itoa(i) + strconv.Itoa(copyNumber))
			h.Write(dhtkeyBytes)
			dhtKey := hex.EncodeToString(h.Sum(nil))
			//fmt.Println("Fetching dht with key: ", dhtKey)
			dhtVal, err := d.g.DhtNode.Get(dhtKey)
			if dhtVal != nil && err == nil {
				dhtData := project.DhtData{Key: dhtKey, Val: dhtVal}
				//fmt.Println("dhtData: ", dhtData)
				d.DecryptChunk(&dhtData)
				//fmt.Println("Decryption ok")
				breakCounter++
				break
			}
		}
	}
	if breakCounter != info.TotChunks {
		fmt.Println("RecoverFile: Some chunks are missing. Try again later")
		return
	}
	d.fileStorageMux.RLock()
	chunks := d.fileStorage[entry].chunks
	d.fileStorageMux.RUnlock()
	fmt.Println("Reassembling file")
	ReconstructFile(chunks, info.FileName, info.TotChunks, rootDownloadedFiles)
}

func (d *Doctor) DecryptChunk(data *project.DhtData) {
	d.fileStorageMux.Lock()
	defer d.fileStorageMux.Unlock()

	//key := data.Key
	val := data.Val

	entry := val.EntryHash //SHA1(Owner||FileName)
	chunkIndex := val.Index
	enChunk := val.EnChunk

	chunkMap, found := d.fileStorage[entry]
	if !found {
		return
	}
	if chunkMap.recovered[chunkIndex] == true {
		//we already have this chunk
		return
	}

	c, err := aes.NewCipher(chunkMap.info.AesKey)
	if err != nil {
		Log(err)
		return
	}
	decChunk := make([]byte, len(enChunk))
	cbc := cipher.NewCBCDecrypter(c, val.Iv)
	cbc.CryptBlocks(decChunk, enChunk)

	decChunk, err = pkcs7Unpad(decChunk, c.BlockSize())
	if err != nil {
		Log(err)
	}
	//fmt.Println(string(decChunk))
	chunkMap.chunks[chunkIndex] = decChunk
	chunkMap.recovered[chunkIndex] = true

	d.fileStorage[entry] = chunkMap
}

//UTILITIES--------------------------------------------------------------------------------------------
func SignMessage(key []byte, Q kyber.Point, m string, id string) (string, *project.FR) {
	H := hmac.New(sha256.New, key)
	//fr wraps pub key and hash
	fr := new(project.FR)
	q, err := Q.MarshalBinary()
	if err != nil {
		Log(err)
		return "", nil
	}
	fr.DPubKey = q
	fr.EntryHash = m
	fr.DoctorId = id
	b, err := json.Marshal(fr)
	if err != nil {
		Log(err)
		return "", nil
	}
	H.Write(b)
	hm := H.Sum(nil)
	return base64.StdEncoding.EncodeToString(hm), fr
}

//Recovers AES key and then decrypts structure
func DecryptPartials(decInfo *DecInfo, p kyber.Scalar) *AuthInfo {
	fmt.Println("All partials received, start decrypting")
	shares := decInfo.shares
	C := decInfo.C
	A := decInfo.A
	t := int(math.Ceil(float64(len(shares)/2) + 1))
	R, err := share.RecoverCommit(suite, shares, t, len(shares))
	if err != nil {
		Log(err)
		return nil
	}
	decPoint := suite.Point().Sub(
		C,
		suite.Point().Sub(
			R,
			suite.Point().Mul(p, A),
		),
	)
	decKey, err := decPoint.Data()
	//fmt.Println("Recovered patient key: ", decKey[:24])
	if err != nil {
		Log(err)
		return nil
	}
	authInfo := new(AuthInfo)
	//fmt.Println("Decrypting info with iv ", decInfo.iv)
	data := DecryptInfo(decKey[:24], decInfo.iv, decInfo.encInfo)
	json.Unmarshal(data, authInfo)
	fmt.Printf("Recovered file info: %v\n", authInfo)
	return authInfo
}

func DecryptInfo(key, iv, ct []byte) []byte {
	c, err := aes.NewCipher(key)
	if err != nil {
		Log(err)
		return nil
	}
	plainPad := make([]byte, len(ct))
	cbc := cipher.NewCBCDecrypter(c, iv)
	cbc.CryptBlocks(plainPad, ct)

	plain, err := pkcs7Unpad(plainPad, c.BlockSize())
	if err != nil {
		Log(err)
		return nil
	}
	return plain
}

//from https://gist.github.com/huyinghuan/7bf174017bf54efb91ece04a48589b22
func pkcs7Unpad(b []byte, blocksize int) ([]byte, error) {
	if blocksize <= 0 {
		return nil, ErrInvalidBlockSize
	}
	if b == nil || len(b) == 0 {
		return nil, ErrInvalidPKCS7Data
	}
	if len(b)%blocksize != 0 {
		return nil, ErrInvalidPKCS7Padding
	}
	c := b[len(b)-1]
	n := int(c)
	if n == 0 || n > len(b) {
		return nil, ErrInvalidPKCS7Padding
	}
	for i := 0; i < n; i++ {
		if b[len(b)-n+i] != c {
			return nil, ErrInvalidPKCS7Padding
		}
	}
	return b[:len(b)-n], nil
}

func ReconstructFile(chunksAsMap map[int][]byte, fileName string, tot int, rootDownloadedFiles string) {
	data := make([]byte, 0)
	chunks := make([][]byte, 0)
	fmt.Println(fileName)
	fmt.Println(rootDownloadedFiles)
	for i := 0; i < tot; i++ {
		chunks = append(chunks, chunksAsMap[i])
	}
	for _, chunk := range chunks {
		data = append(data, chunk...)
	}
	fmt.Println(string(data))
	filePath := rootDownloadedFiles + "/" + filepath.Base(fileName)
	ioutil.WriteFile(filePath, data, 0644)
}
