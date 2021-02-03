package gossip

import (
	"encoding/hex"
	"sync"

	"go.dedis.ch/cs438/hw3/gossip/types"
)

//BLOCKCHAIN-------------------------------------------------------------------------------
type BlockChain struct {
	mux sync.RWMutex
	ch  map[string]types.Block //block_hash -> block
	len int
	// chans  map[string]chan types.Block //filename -> channel for the listener to send the accepted block if contains filename
}

func (bc *BlockChain) New() *BlockChain {
	bc.ch = make(map[string]types.Block)
	bc.len = 0
	// bc.chans = make(map[string]chan types.Block)
	// bc.naming = make(map[string]string)
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
func (bc *BlockChain) AddBlock(acceptedBlock types.Block, g *Gossiper) {

	//Should I handle blocks with blockNumber < bc.len?
	bc.mux.Lock()
	defer bc.mux.Unlock()

	//Some checks
	if acceptedBlock.BlockNumber < bc.len {
		//already put in blockchain
		return
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
