package gossip

import (
	"math/rand"
	"sync"
)

// NodesList stores the discovered nodes as a map of identifiers to booleans
// (basically it's a set, so can add a node without needing to check for duplicates)
// We use a lock to sync concurrent reads / writes
type NodesList struct {
	exists   map[string]bool // "ip:port" -> exists?
	addrList []string        // "ip:port", "ip:port", "ip:port" ...
	sync.RWMutex
}

func (nl *NodesList) addAddresses(addresses []string) error {
	nl.Lock()
	defer nl.Unlock()
	for _, a := range addresses {
		if nl.exists[a] != true {
			// add to list of known nodes
			nl.addrList = append(nl.addrList, a)
		}
		nl.exists[a] = true
	}
	return nil
}

func (nl *NodesList) getNodes() []string {
	nl.RLock()
	defer nl.RUnlock()
	return nl.addrList
}

func (nl *NodesList) getRandom() (string, bool) {
	nl.RLock()
	defer nl.RUnlock()
	if len(nl.addrList) == 0 {
		return "", false
	}
	idx := rand.Intn(len(nl.addrList))
	return nl.addrList[idx], true
}

func (nl *NodesList) getRandomExcept(peer []string) (string, bool) {
	nl.RLock()
	defer nl.RUnlock()

	except := make(map[string]bool)
	for _, p := range peer {
		except[p] = true
	}

	possiblePeers := make([]string, 0)
	for known := range nl.exists {
		if _, cantAdd := except[known]; !cantAdd {
			possiblePeers = append(possiblePeers, known)
		}
	}

	if len(possiblePeers) == 0 {
		return "", false
	}

	idx := rand.Intn(len(possiblePeers))
	return possiblePeers[idx], true
}

func (nl *NodesList) print() {
	//nl.RLock()
	//defer nl.RUnlock()
	//log.Lvlf1("PEERS %s\n", strings.Join(nl.addrList, ","))
}
