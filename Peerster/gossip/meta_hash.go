package gossip

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.dedis.ch/onet/v3/log"
)

const chunk_size = 8192 //8Kib

type fileInfo struct {
	filePath string
	metaFile string
	//slices to be mantained in a parallel fashion: chunkhashes[i] == hash(chunks[i])
	chunks       [][]byte //plaintext chunks
	inPossession []bool   //handles case where different chunks have same hash
	chunkHashes  [][]byte //hashes of chunks
	chunksNum    int
}

type MetaHash struct {
	HashTable map[string]*fileInfo //hex_string(MetaHash)->path to file, MetaFile name, chunksNum, list of chunks, list of chunks hashes
	counter   uint32               //for generating unique names for metafiles
	mux       sync.RWMutex
}

func (m *MetaHash) New() *MetaHash {
	HashTable := make(map[string]*fileInfo)
	m.HashTable = HashTable
	m.counter = 0
	return m
}

//Given a file from client, indexes it and generates metafile
func (m *MetaHash) GenerateMetaFile(g *Gossiper, filePath string, shareDir string) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}
	stat, err := os.Stat(filePath)
	if err != nil {
		log.Fatal(err)
	}

	//copy the file into shared folder
	ioutil.WriteFile(shareDir+"/"+stat.Name(), data, os.ModePerm)

	chunks, chunkHashes := ReadFileByChunks(data)
	inPossession := make([]bool, len(chunkHashes))
	for i := 0; i < len(inPossession); i++ {
		inPossession[i] = true
	}

	//create a new MetaFile
	H := sha256.New()
	m.counter++
	metaName := shareDir + "/" + "MetaFile" + fmt.Sprint(m.counter) + ".bin"
	//write all chunks hashes in MetaFile
	content := make([]byte, 0)
	for _, hash := range chunkHashes {
		content = append(content, hash...)
		H.Write(hash)
	}
	ioutil.WriteFile(metaName, content, os.ModePerm)
	//compute the MetaHash as hex string
	metaHash := hex.EncodeToString(H.Sum(nil))

	//check if we have a file with the same content (metahash). If not add it
	m.mux.Lock()
	for metahash, _ := range m.HashTable {
		if metahash == metaHash {
			//we already have this file
			os.Remove(shareDir + "/" + filepath.Base(metaName))
			m.counter--
			return
		}
	}
	m.mux.Unlock()

	//CONSENSUS
	consensusName, ok := g.bc.RetrieveNameFromBlocks(filepath.Base(filePath), metaHash, g)
	if !ok {
		return
	}
	m.mux.Lock()
	filePath = filepath.Dir(filePath) + "/" + consensusName
	fi := &fileInfo{filePath: shareDir + "/" + filepath.Base(filePath), metaFile: metaName, chunks: chunks, inPossession: inPossession, chunkHashes: chunkHashes}
	m.HashTable[metaHash] = fi
	m.mux.Unlock()
}

func (m *MetaHash) GetIndexedFiles() []*File {
	//a test seems to fail if this lock is enabled
	//m.mux.RLock()
	//defer m.mux.RUnlock()
	files := make([]*File, 0)
	for metahash, fi := range m.HashTable {
		files = append(files, &File{Name: filepath.Base(fi.filePath), MetaHash: metahash})
	}
	return files
}

func (m *MetaHash) QueryMetaHash(hash string) []byte {
	//Retrieve the data associated to the requested hash:
	//-> metaFile if hash is metahash
	//-> chunk if hash is a chunk hash
	//m.mux.RLock()
	//defer m.mux.RUnlock()
	for metaHash, fi := range m.HashTable {
		if hash == metaHash {
			//request for metafile content
			buf := make([]byte, 0)
			for _, chunkHash := range fi.chunkHashes {
				buf = append(buf, chunkHash...)
			}
			return buf
		} else {
			//check if it is a request for a specific chunk
			for i, chunkHash := range fi.chunkHashes {
				if hex.EncodeToString(chunkHash) == hash {
					return fi.chunks[i]
				}
			}
		}
	}
	//we do not have the request hash: drop
	log.Error("Requested hash not found:", hash)
	return nil
}

func (m *MetaHash) AddToMetaHash(downloadsDir, filename, hash string, data []byte) (bool, bool) {
	//adds chunk to entry of hashtable or creates entry and metafile
	//return isMeta, isChunk

	m.mux.Lock()
	defer m.mux.Unlock()
	//check if it's a chunk
	for metaHash, fi := range m.HashTable {
		if hash == metaHash {
			log.Error("MetaHash already stored")
			return false, false
		} else {
			for i, chunkHash := range fi.chunkHashes {
				if hex.EncodeToString(chunkHash) == hash && fi.inPossession[i] == false {
					fi.chunks[i] = data //what if chunks has len < i? This should not be possible btw
					fi.inPossession[i] = true
					fi.chunksNum++
					if fi.chunksNum == len(fi.chunkHashes) {
						//we have a complete file now
						m.ReconstructFile(downloadsDir+"/"+filepath.Base(filename), fi.chunks)
					}
					return false, true
				}
			}
		}
	}
	//it's a new MetaFile

	//store chunks hashes
	chunkHashes := make([][]byte, 0)
	inPossession := make([]bool, 0)
	buf := make([]byte, 32)
	for i, b := range data {
		buf[i%32] = b
		if (i+1)%32 == 0 {
			chunkHashes = append(chunkHashes, buf)
			inPossession = append(inPossession, false)
			buf = make([]byte, 32)
		}
	}

	//create and write MetaFile
	H := sha256.New()
	metaName := downloadsDir + "/" + "MetaFile" + fmt.Sprint(m.counter) + ".bin"
	content := make([]byte, 0)
	for _, hash := range chunkHashes {
		content = append(content, hash...)
		H.Write(hash)
	}
	ioutil.WriteFile(metaName, content, os.ModePerm)

	m.HashTable[hash] = &fileInfo{filePath: downloadsDir + "/" + filename, metaFile: metaName, chunks: make([][]byte, len(chunkHashes)), inPossession: inPossession, chunkHashes: chunkHashes, chunksNum: 0}
	m.counter++
	return true, false
}

func (m *MetaHash) ReconstructFile(filePath string, chunks [][]byte) {
	data := make([]byte, 0)
	for _, chunk := range chunks {
		data = append(data, chunk...)
	}
	ioutil.WriteFile(filePath, data, os.ModePerm)
}

func (m *MetaHash) RemoveChunk(hash string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	for metahash, fi := range m.HashTable {
		if metahash == hash {
			delete(m.HashTable, metahash)
			return
		} else {
			for i, chunkhash := range fi.chunkHashes {
				if hex.EncodeToString(chunkhash[:]) == hash {
					fi.chunks[i] = nil
					fi.inPossession[i] = false
					fi.chunksNum--
					return
				}
			}
		}
	}
}

func (m *MetaHash) Rebuild(shareDir, DownloadDir string) {
	//make a list with all the files in the shareDir and DownloadDir
	fileList := make([]string, 0)
	e := filepath.Walk(shareDir, func(path string, f os.FileInfo, err error) error {
		if f.IsDir() == false && strings.Contains(f.Name(), "indexedState.json") == false {
			fileList = append(fileList, path)
			return err
		}
		return err
	})
	if e != nil {
		panic(e)
	}
	e = filepath.Walk(DownloadDir, func(path string, f os.FileInfo, err error) error {
		if f.IsDir() == false {
			fileList = append(fileList, path)
			return err
		}
		return err
	})
	if e != nil {
		panic(e)
	}

	//for each file f
	for _, f := range fileList {
		if strings.Contains(filepath.Base(f), "MetaFile") == false {
			data, err := ioutil.ReadFile(shareDir + "/" + filepath.Base(f))
			if err != nil {
				log.Fatal(err)
			}
			chunks, chunkHashes := ReadFileByChunks(data)
			inPossession := make([]bool, len(chunkHashes))
			for i := 0; i < len(inPossession); i++ {
				inPossession[i] = true
			}
			//compute metaHash
			H := sha256.New()
			for _, hash := range chunkHashes {
				H.Write(hash)
			}
			metaHash := hex.EncodeToString(H.Sum(nil))

			//find the metaFile for this file
			for _, metaFile := range fileList {
				if strings.Contains(metaFile, "MetaFile") {
					H := sha256.New()
					//read this metaFile and compute its hash
					data, err := ioutil.ReadFile(shareDir + "/" + filepath.Base(metaFile))
					if err != nil {
						log.Error("Failed to open ", metaFile)
					}
					computedChunkHashes := make([][]byte, 0)
					buf := make([]byte, 32)
					for i, b := range data {
						buf[i%32] = b
						if (i+1)%32 == 0 {
							computedChunkHashes = append(computedChunkHashes, buf)
							buf = make([]byte, 32) //we need this
						}
					}
					for _, hash := range computedChunkHashes {
						H.Write(hash)
					}
					computed_metaHash := hex.EncodeToString(H.Sum(nil))
					if computed_metaHash == metaHash {
						//add entry into hashtable, we have found the right metaFile for f
						m.mux.Lock()
						fi := &fileInfo{filePath: f, metaFile: metaFile, chunks: chunks, inPossession: inPossession, chunkHashes: chunkHashes}
						m.HashTable[metaHash] = fi
						m.counter++
						m.mux.Unlock()
					}
				}
			}
			//if we haven't found the metaFile for this file, create it
			m.mux.Lock()
			if _, found := m.HashTable[metaHash]; !found {
				//generate MetaFile
				H := sha256.New()
				metaName := shareDir + "/" + "MetaFile" + fmt.Sprint(m.counter) + ".bin"
				content := make([]byte, 0)
				for _, hash := range chunkHashes {
					content = append(content, hash...)
					H.Write(hash)
				}
				ioutil.WriteFile(metaName, content, os.ModePerm)
				m.HashTable[metaHash] = &fileInfo{filePath: shareDir + "/" + filepath.Base(f), metaFile: metaName, chunks: chunks, chunkHashes: chunkHashes, chunksNum: len(chunkHashes)}
				m.counter++
			}
			m.mux.Unlock()
		}
	}
}

func (m *MetaHash) ReturnMetaHash() map[string]*fileInfo {
	m.mux.RLock()
	defer m.mux.RUnlock()
	return m.HashTable
}

//Utilities-------------------------------------------------------------------------------------------
func ReadFileByChunks(data []byte) ([][]byte, [][]byte) {
	chunks := make([][]byte, 0)
	chunkHashes := make([][]byte, 0)
	buf := make([]byte, 0)
	for i, b := range data {
		buf = append(buf, b)
		if (i+1)%chunk_size == 0 {
			h := sha256.New()
			h.Write(buf[:])
			chunks = append(chunks, buf[:])
			chunkHashes = append(chunkHashes, h.Sum(nil))
			buf = make([]byte, 0)
		}
	}
	if len(data)%chunk_size != 0 {
		//final dump
		h := sha256.New()
		h.Write(buf[:])
		chunks = append(chunks, buf[:])
		chunkHashes = append(chunkHashes, h.Sum(nil))
	}
	return chunks, chunkHashes
}
