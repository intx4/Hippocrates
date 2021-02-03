package gossip

import (
	"encoding/hex"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/types"
)

//BLOCKCHAIN-------------------------------------------------------------------------------
type BlockChain struct {
	mux    sync.RWMutex
	ch     map[string]types.Block //block_hash -> block
	len    int
	chans  map[string]chan types.Block //filename -> channel for the listener to send the accepted block if contains filename
	naming map[string]string           //expose metahash->name
}

func (bc *BlockChain) New() *BlockChain {
	bc.ch = make(map[string]types.Block)
	bc.len = 0
	bc.chans = make(map[string]chan types.Block)
	bc.naming = make(map[string]string)
	return bc
}
func (bc *BlockChain) GetBlockNum() int {
	bc.mux.RLock()
	defer bc.mux.RUnlock()
	return bc.len
}

func (bc *BlockChain) GetBlocks() (string, map[string]types.Block) {
	bc.mux.RLock()
	defer bc.mux.RUnlock()
	lastHash := ""
	for key, value := range bc.ch {
		if value.BlockNumber == bc.len-1 {
			lastHash = key
			break
		}
	}
	return lastHash, bc.ch
}
func (bc *BlockChain) GetLength() int {
	bc.mux.RLock()
	defer bc.mux.RUnlock()
	return bc.len
}
func (bc *BlockChain) QueryName(Metahash string) (string, bool) {
	bc.mux.RLock()
	defer bc.mux.RUnlock()
	name, found := bc.naming[Metahash]
	return name, found
}
func (bc *BlockChain) CheckProposal(FileName, MetaHash string) bool {
	//Check if in the blockchain there is already a name or a metahash like
	//proposed ones
	bc.mux.RLock()
	defer bc.mux.RUnlock()

	for mh, name := range bc.naming {
		if mh == MetaHash || name == FileName {
			return false
		}
	}
	return true
}
func (bc *BlockChain) RetrieveNameFromBlocks(Filename, Metahash string, g *Gossiper) (string, bool) {
	// This function tries to retrieve the filename from the blockchain if it exists,
	// otheriwse it will start a PaxosRun to add a new block with such a name

	//check if we have a name for this metahash
	if name, found := bc.QueryName(Metahash); found {
		return name, true
	}
	//prevents proposing a bad value
	if ok := bc.CheckProposal(Filename, Metahash); !ok {
		return "", false
	}
	acceptedMetahash := ""
	prevLen := bc.GetLength()
	lastBlockHashBytes := make([]byte, 32)
	//retry if the acceptedBlock do not clash with my proposal
	for acceptedMetahash != Metahash {

		lastBlockHash, _ := bc.GetLastBlock()
		if lastBlockHash != "" {
			lastBlockHashBytes, _ = hex.DecodeString(lastBlockHash)
		}
		len := bc.GetLength()
		//start Paxos algo
		go g.PaxosRun(Filename, Metahash, len, lastBlockHashBytes)

		for len == prevLen {
			time.Sleep(500 * time.Millisecond)
			len = bc.GetLength()
		}
		prevLen = len
		_, last := bc.GetLastBlock()
		acceptedMetahash = hex.EncodeToString(last.Metahash)
	}
	return bc.naming[Metahash], true
}
func (bc *BlockChain) AddBlock(acceptedBlock types.Block, g *Gossiper) {

	//Should I handle blocks with blockNumber < bc.len?
	bc.mux.Lock()
	defer bc.mux.Unlock()

	//Some checks
	if acceptedBlock.BlockNumber < bc.len {
		//already put in blockchain
		return
	}
	name := acceptedBlock.Filename
	metahash := acceptedBlock.Metahash
	for _, value := range bc.ch {
		if hex.EncodeToString(value.Metahash) == hex.EncodeToString(metahash) {
			return //already stored
		}
		if value.Filename == name {
			return //already stored
		}
	}

	if bc.len == 0 {
		hash := make([]byte, 32)
		acceptedBlock.PreviousHash = hash
	} else {
		for _, value := range bc.ch {
			if value.BlockNumber == bc.len-1 {
				acceptedBlock.PreviousHash = value.Hash()
			}
		}
	}
	acceptedBlock.BlockNumber = bc.len
	bc.len++
	bc.ch[hex.EncodeToString(acceptedBlock.Hash())] = acceptedBlock

	//update naming logic
	bc.naming[hex.EncodeToString(acceptedBlock.Metahash)] = acceptedBlock.Filename

	//add new round
	g.paxosRounds[bc.len] = new(PaxosRound).New()

	return
}

func (bc *BlockChain) GetLastBlock() (string, types.Block) {
	bc.mux.Lock()
	defer bc.mux.Unlock()
	for key, value := range bc.ch {
		if value.BlockNumber == bc.len-1 {
			return key, value
		}
	}
	return "", types.Block{}
}
