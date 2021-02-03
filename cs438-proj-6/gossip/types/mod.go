// Package types contains the type messages for Paxos and TLC. We use a separate
// package to avoid import cycles.
package types

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
)

// Paxos messages. Feel free to move that in a separate file and/or package.

type FileAccess struct {
	File    string `json:"file"` // SHA1(OwnerID||Filename)
	Time    int64  `json:"time"`
	User    string `json:"user"`    //Doctor's ID
	Address string `json:"address"` //Doctor Address
	Pubkey  []byte `json:"pubkey"`  //Doctor Public Key: to make a point []byte, just call bytes,err := PubKey.MarshalBinary()
}

// ExtraMessage is carried by a rumor message.
type ExtraMessage struct {
	PaxosPrepare *PaxosPrepare `json:"prepare"`
	PaxosPromise *PaxosPromise `json:"promise"`
	PaxosPropose *PaxosPropose `json:"propose"`
	PaxosAccept  *PaxosAccept  `json:"accept"`
	TLC          *TLC          `json:"tlc"`
}

// my function
func (extra *ExtraMessage) NewPaxosMsg(IDp, IDa, SeqID int, block Block) {
	if extra.PaxosAccept != nil {
		extra.PaxosAccept = &PaxosAccept{PaxosSeqID: SeqID, ID: IDp, Value: block}
	} else if extra.PaxosPrepare != nil {
		extra.PaxosPrepare = &PaxosPrepare{PaxosSeqID: SeqID, ID: IDp}
	} else if extra.PaxosPromise != nil {
		extra.PaxosPromise = &PaxosPromise{PaxosSeqID: SeqID, IDp: IDp, IDa: IDa, Value: block}
	} else if extra.PaxosPropose != nil {
		extra.PaxosPropose = &PaxosPropose{PaxosSeqID: SeqID, ID: IDp, Value: block}
	}
}

// Copy performs a deep copy of extra message
func (e *ExtraMessage) Copy() *ExtraMessage {
	var paxosPrepare *PaxosPrepare
	var paxosPromise *PaxosPromise
	var paxosPropose *PaxosPropose
	var paxosAccept *PaxosAccept
	var tlc *TLC

	if e.PaxosPrepare != nil {
		paxosPrepare = new(PaxosPrepare)
		paxosPrepare.PaxosSeqID = e.PaxosPrepare.PaxosSeqID
		paxosPrepare.ID = e.PaxosPrepare.ID
	}

	if e.PaxosPromise != nil {
		paxosPromise = new(PaxosPromise)
		paxosPromise.PaxosSeqID = e.PaxosPromise.PaxosSeqID
		paxosPromise.IDp = e.PaxosPromise.IDp
		paxosPromise.IDa = e.PaxosPromise.IDa
		paxosPromise.Value = *(e.PaxosPromise.Value.Copy())
	}

	if e.PaxosPropose != nil {
		paxosPropose = new(PaxosPropose)
		paxosPropose.PaxosSeqID = e.PaxosPropose.PaxosSeqID
		paxosPropose.ID = e.PaxosPropose.ID
		paxosPropose.Value = *(e.PaxosPropose.Value.Copy())
	}

	if e.PaxosAccept != nil {
		paxosAccept = new(PaxosAccept)
		paxosAccept.PaxosSeqID = e.PaxosAccept.PaxosSeqID
		paxosAccept.ID = e.PaxosAccept.ID
		paxosAccept.Value = *(e.PaxosAccept.Value.Copy())
	}

	if e.TLC != nil {
		tlc = new(TLC)
		tlc.Block = *e.TLC.Block.Copy()
	}

	return &ExtraMessage{
		PaxosPrepare: paxosPrepare,
		PaxosPromise: paxosPromise,
		PaxosPropose: paxosPropose,
		PaxosAccept:  paxosAccept,
		TLC:          tlc,
	}
}

func (extra *ExtraMessage) Encrypt(key []byte) *[]byte {
	// encode the data
	bytes, err := json.Marshal(extra)
	if err != nil {
		panic(err)
	}
	ciphertext, err := encrypt(bytes, key)
	if err != nil {
		// TODO: Properly handle error
		log.Fatal(err)
	}
	return &ciphertext
}

func (extra *ExtraMessage) Decrypt(ciphertext *[]byte, key []byte) {
	//decrypt data
	plaintext, err := decrypt(*ciphertext, key)
	if err != nil {
		// TODO: Properly handle error
		log.Fatal(err)
	}
	// decode the data
	if err := json.Unmarshal(plaintext, extra); err != nil {
		panic(err)
	}
}

func encrypt(plaintext []byte, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

// PaxosPrepare describes a PREPARE request to an acceptor.
type PaxosPrepare struct {
	PaxosSeqID int `json:"paxosseqid"`
	ID         int `json:"id"`
}

// PaxosPromise describes a PROMISE request made by an acceptor to a proposer.
// IDp is the ID the proposer sent. IDa is the highest ID the acceptor saw and
// Value is the value it commits to, if any.
// Value/ID
type PaxosPromise struct {
	PaxosSeqID int `json:"paxosseqid"`
	IDp        int `json:"idp"`

	IDa   int   `json:"ida"`
	Value Block `json:"value"`
}

// PaxosPropose describes a PROPOSE request made by a proposer to an ACCEPTOR.
type PaxosPropose struct {
	PaxosSeqID int `json:"paxosseqid"`
	ID         int `json:"id"`

	Value Block `json:"value"`
}

// PaxosAccept describes an ACCEPT request that is sent by an acceptor to its
// proposer and all the learners.
type PaxosAccept struct {
	PaxosSeqID int `json:"paxosseqid"`
	ID         int `json:"id"`

	Value Block `json:"value"`
}

// TLC is the message sent by a node when it knows concensus has been reached
// for that block.
type TLC struct {
	Block Block `json:"block"`
}

// Blockchain data structures. Feel free to move that in a separate file and/or
// package.

// Block describes the content of a block in the blockchain.
type Block struct {
	BlockNumber  int    `json:"blocknum"` // not included in the hash
	PreviousHash []byte `json:"prevhash"`

	Access FileAccess `json:"access"`
}

// Hash returns the hash of a block. It doesn't take the index.
func (b Block) Hash() []byte {
	h := sha256.New()

	h.Write(b.PreviousHash)

	h.Write([]byte(b.Access.File))

	time := make([]byte, 8)
	binary.LittleEndian.PutUint64(time, uint64(b.Access.Time))
	h.Write(time)

	h.Write([]byte(b.Access.User))
	h.Write([]byte(b.Access.Address))

	// TODO: is this right? Solved. Just use []byte in packets as Points. For details see FileAccess
	d := b.Access.Pubkey
	h.Write(d)

	return h.Sum(nil)
}

// Copy performs a deep copy of a block
func (b Block) Copy() *Block {
	return &Block{
		BlockNumber:  b.BlockNumber,
		PreviousHash: append([]byte{}, b.PreviousHash...),

		Access: b.Access,
	}
}
