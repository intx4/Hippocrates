package project

import (
	dkg "go.dedis.ch/kyber/v3/share/dkg/pedersen"
)

//Wrapped inside GossipPacket
type HippoMsg struct {
	//Wraps messages for Dkg Protocol
	DkgMsg *DkgMessage `json:"dkgmsg"`
	//Wraps every message Type Exchanged in the project between cothority and other nodes
	PkReq    *PkRequest      `json:"pkreq"`
	PkReply  *PkReply        `json:"pkreply"`
	EncFileI *EncFileInfo    `json:"encfilei"`
	FileReq  *FileRequest    `json:"filereq"`
	PartDec  *PartialDecrypt `json:"partdec"`
	ChunkReq *ChunkRequest   `json:"chunkreq"`
	EncChunk *DhtEncChunk    `json:"dhtencchunk"`
	//TO DO: add a packet for patient delivering values into the dht
}

type PkRequest struct {
	//Request for cothority pk
	Req bool `json:"req"`
}
type PkReply struct {
	//Reply from cothority
	Pk      []byte `json:"pk"`      //binary of point
	Granted bool   `json:"granted"` //if the key has been granted
}
type EncFileInfo struct {
	//Wraps information sent from patient to cothority
	Points    *Points `json:"points"`
	EntryHash string  `json:"entryhash"` //hex of Sha1(OwnerID||Filename)
}

type Points struct {
	C []byte `json:"c"` //marshal binary of point
	U []byte `json:"u"`
}

type FileRequest struct {
	//Wraps the request for a file from a doctor
	Fr   *FR    `json:"fr"`
	Hmac string `json:"hmac"` //it's the b64 encoding of the HMAC of (Fr)

}
type FR struct {
	//Wrapper for Hmac in FileRequest
	DoctorId  string `json:"doctorid"`
	DPubKey   []byte `json:"dpubkey"` //binary of point
	EntryHash string `json:"entryhash"`
}
type PartialDecrypt struct {
	//Wraps the partial decryption of file info from cothority to doctor
	EntryHash string `json:"entryhash"` //SHA1(Owner||FileName)
	A         []byte `json: "A"`        //dkg public key
	PubShareI int    `json:"pubshareI"` //public share I
	PubShareV []byte `json:"pubshareV"` //public share V (point)
	C         []byte `json:"C"`         //C of encrypted file
}
type ChunkRequest struct {
	// Wraps a request for a chunk from a doctor to a dht node
	Key string `json:"key"` //hex of SHA1(User||FileName||Chunk_index||copyNum)
}
type DhtEncChunk struct {
	// Wraps an Encrypted Chunk recovered from the DHT
	// Basically is the struct that should be sent in the Get(key) of a dht node
	Data *DhtData `json:"dhtdata"`
}
type DhtData struct {
	//Wrapper for sending data into chord
	Key string  `json:"key"` //Key of the DHT: hex of SHA1(gossiper.ID||Filename||chunk_index||copy_num)
	Val *DhtVal `json:"val"` //Val to be stored in the DHT
}
type DhtVal struct {
	Index     int    `json:"index"`     //index of chunk
	EntryHash string `json:"entryhash"` // SHA1(gossiper.ID||FileName)
	EnChunk   []byte `json:"enChunk"`   //encrypted chunk
	Iv        []byte `json:"iv"`        //iv for aes
}

//DKG PROTOCOL--------------------------------------------------------------------------------------------

//Wrapped inside GossipPacket -> HippoMsg
type DkgMessage struct {
	//Wraps message exchanged in cothority nodes for the setup of dkg protocol
	DealMsg *DealMessage `json:"dealmsg"`
}

type DealMessage struct {
	Deal *dkg.Deal `json:"deal"`
	To   uint32    `json:"to"` //index of the (supposed) receiver of the deal
}

//Wrapped inside Rumor
type DealResponse struct {
	Resp *dkg.Response `json:"response"`
	From uint32        `json:"from"` //index of the sender of the response
}

//Wrapped inside Rumor
type PkMessage struct {
	//broadcast public key of one node when initializing cothority
	Index  uint32 `json:"index"`
	PubKey []byte `json:"pubkey"` //binary marshal of point
}

//Utilities-----------------------------------------------------

/*
func (dkgmsg *DkgMessage) Copy() *DkgMessage {
	var DealMsg *DealMessage
	var Response *DealResponse
	var PubKeyMsg *PkMessage

	if dkgmsg.DealMsg != nil {
		DealMsg = new(DealMessage)
		DealMsg.To = dkgmsg.DealMsg.To
		*(DealMsg.Deal) = *(dkgmsg.DealMsg.Deal)
		return &DkgMessage{DealMsg: DealMsg}
	} else if dkgmsg.Response != nil {
		Response = new(DealResponse)
		Response.From = dkgmsg.Response.From
		*(Response.Resp) = *(dkgmsg.Response.Resp)
		return &DkgMessage{Response: Response}
	} else if dkgmsg.PubKeyMsg != nil {
		PubKeyMsg = new(PkMessage)
		PubKeyMsg.Index = dkgmsg.PubKeyMsg.Index
		PubKeyMsg.PubKey = dkgmsg.PubKeyMsg.PubKey
		return &DkgMessage{PubKeyMsg: PubKeyMsg}
	}
	return nil
}
*/
