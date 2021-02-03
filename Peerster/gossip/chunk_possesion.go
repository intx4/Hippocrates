package gossip

import (
	"sync"

	"go.dedis.ch/onet/v3/log"
)

type ChunkInfo struct {
	chunksOfpeer   map[string][]uint32 //peerId -> chunks that peer has for file with metahash key of rcp
	chunksPossesed []uint32            //list of all chunks ids seen for the file
	metaFile       []byte              //metaFile retrieved when knowing about the file
	total          int                 //total chunks of file
}

type ChunkPossesion struct {
	mux sync.RWMutex
	rcp map[string]*ChunkInfo //metahash->chunkinfo
}

func (ci *ChunkInfo) New() *ChunkInfo {
	ci.chunksOfpeer = make(map[string][]uint32)
	ci.chunksPossesed = make([]uint32, 0)
	ci.total = 0
	ci.metaFile = make([]byte, 0)
	return ci
}
func (cp *ChunkPossesion) New() *ChunkPossesion {
	cp.rcp = make(map[string]*ChunkInfo)
	return cp
}

func (cp *ChunkPossesion) GetRemoteChunksPossession(metaHash string) map[string][]uint32 {
	cp.mux.RLock()
	defer cp.mux.RUnlock()
	return cp.rcp[metaHash].chunksOfpeer
}

func (cp *ChunkPossesion) Update(metaHash string, peerId string, chunks []uint32, g *Gossiper) bool {

	//return true if we get a total match
	cp.mux.Lock()
	defer cp.mux.Unlock()
	if _, found := cp.rcp[metaHash]; !found {
		cp.rcp[metaHash] = new(ChunkInfo)
		cp.rcp[metaHash].New()
	}
	//add new peerid which has chunks
	cp.rcp[metaHash].chunksOfpeer[peerId] = chunks
	//update list of possesed chunks
	for _, chunkId := range chunks {
		found := false
		for _, possesed := range cp.rcp[metaHash].chunksPossesed {
			if possesed == chunkId {
				found = true
				break
			}
		}
		if !found {
			cp.rcp[metaHash].chunksPossesed = append(cp.rcp[metaHash].chunksPossesed, chunkId)
		}
	}
	if cp.rcp[metaHash].total == 0 {
		if metaFile, err := g.RetrieveMetaFile(peerId, metaHash); err == nil {
			cp.rcp[metaHash].total = len(metaFile) / 32
			cp.rcp[metaHash].metaFile = metaFile
		} else {
			log.Error("Failed to retrieve MetaFile")
		}
	} else if cp.rcp[metaHash].total == len(cp.rcp[metaHash].chunksPossesed) {
		return true
	}
	return false
}
func (cp *ChunkPossesion) RetrieveMetaFile(metaHash string) ([]byte, bool) {
	cp.mux.RLock()
	defer cp.mux.RUnlock()
	if len(cp.rcp[metaHash].metaFile) > 0 {
		return cp.rcp[metaHash].metaFile, true
	} else {
		return nil, false
	}
}
