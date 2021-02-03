package gossip

import (
	"sync"
	"time"
)

type Key struct {
	origin   string
	keywords []string
}

type SearchState struct {
	/*The idea for using this is that in AddSearchMessage the thread will receive
	in the channel the MetaHashes of those files for which there is a total match
	The Preprocessing is done in the handler which knows if a filename satisfies a keyword */
	mux  sync.RWMutex
	reqs map[*Key]*time.Timer //comma separated keywords -> chan string
}

func (st *SearchState) New() *SearchState {
	st.reqs = make(map[*Key]*time.Timer)
	return st
}
func (st *SearchState) Add(r *SearchRequest) bool {
	//it receives a
	st.mux.Lock()
	defer st.mux.Unlock()
	key := new(Key)
	key.keywords = r.Keywords
	key.origin = r.Origin

	for k, _ := range st.reqs {
		if k.origin == key.origin && len(key.keywords) == len(k.keywords) {
			for i := 0; i < len(k.keywords); i++ {
				if key.keywords[i] != k.keywords[i] {
					break
				}
			}
			key = k
			break
		}
	}
	if t, found := st.reqs[key]; !found {
		t := time.NewTimer(500 * time.Millisecond)
		st.reqs[key] = t
		return true
	} else {
		select {
		case <-t.C:
			//the last request was older than 0.5 Second
			st.reqs[key] = time.NewTimer(500 * time.Millisecond)
			return true
		default:
			//the request is still fresh
			return false
		}
	}

}
