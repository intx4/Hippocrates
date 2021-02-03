package gossip

import (
	"math"
	"sync"

	"github.com/google/go-cmp/cmp"
	"go.dedis.ch/cs438/hw3/gossip/types"
)

//PAXOS--------------------------------------------------------------------------------
type Proposer struct {
	mux       sync.RWMutex
	promiseDS map[int]chan (*types.PaxosPromise) //this will receive promises from acceptors
	acceptDS  map[int]chan types.Block           //this will receive a boolean by the listener logic if it will collect enough accepts
	//not used
	consensusChan chan bool //used by listener to notify that consensus was reached by TLC. Fastforward
	consensusTlc  bool
	phase         int //which phase we are in
}

type Acceptor struct {
	mux             sync.RWMutex
	highestIDp      int         //highest value for promises
	highestIDa      int         //highest value accepted
	highestIdaValue types.Block //value of the accepted proposal, goes with IDa
}
type CountTlc struct {
	value     types.Block
	c         int
	consensus bool
}
type Listener struct {
	mux               sync.RWMutex
	listenerDSAccepts (map[int]int) //map of IDp -> number of accepts received
	//when the #Accepts is enough, he will notify the Proposer if the acceptDS entry of the IDp exists
	listenerDSTlc        []*CountTlc //PaxosSeqID -> blocks with the consensus + number of tlc received
	consensusOnceAccepts bool
	consensusOnceTlc     bool
	//once variables will make the consensus condition (something >= threshold) true only once
}
type PaxosRound struct {
	a *Acceptor
	l *Listener
	p *Proposer
}

func (pr *PaxosRound) New() *PaxosRound {
	pr.a = new(Acceptor)
	pr.a = pr.a.New()
	pr.l = new(Listener)
	pr.l = pr.l.New()
	pr.p = new(Proposer)
	pr.p = pr.p.New()
	return pr
}

//LISTENER-----------------------------------------------------------------------------------
func (l *Listener) New() *Listener {
	l.listenerDSAccepts = make(map[int]int)
	l.listenerDSTlc = make([]*CountTlc, 0)
	l.consensusOnceAccepts = false
	l.consensusOnceTlc = false
	return l
}
func (l *Listener) NewEntryWithSeqNum(SeqID int) {
	l.listenerDSAccepts = make(map[int]int)
	l.listenerDSTlc = make([]*CountTlc, 0)
}
func (l *Listener) NewEntryWithKey(Key int) {
	l.listenerDSAccepts[Key] = 0
}
func (l *Listener) UpdateEntry_Accepts(Key int, N int) (bool, bool) {
	l.mux.Lock()
	defer l.mux.Unlock()
	threshold := int(math.Floor(float64(N/2) + 1))

	if _, found := l.listenerDSAccepts[Key]; !found {
		l.NewEntryWithKey(Key)
	}

	l.listenerDSAccepts[Key] = l.listenerDSAccepts[Key] + 1

	if l.listenerDSAccepts[Key] >= threshold {
		if l.consensusOnceAccepts == false {
			l.consensusOnceAccepts = true
			return true, true //consensus + once
		}
		return true, false
	}
	return false, false
}
func (l *Listener) UpdateEntry_Tlc(SeqID, N int, tlcBlock types.Block) (bool, bool) {
	l.mux.Lock()
	defer l.mux.Unlock()

	registered := false
	if l.consensusOnceTlc {
		//we have to choose only one value
		return false, false
	}
	for _, blocks := range l.listenerDSTlc {
		if cmp.Equal(tlcBlock.Access, blocks.value.Access) {
			registered = true
		}
	}
	if !registered {
		l.listenerDSTlc = append(l.listenerDSTlc, &CountTlc{value: tlcBlock, c: 0, consensus: false})
	}
	for _, blocks := range l.listenerDSTlc {
		if blocks.consensus == false {
			blocks.c++
			threshold := int(math.Floor(float64(N/2) + 1))
			if blocks.c >= threshold {
				blocks.consensus = true
				if l.consensusOnceTlc == false {
					l.consensusOnceTlc = true
					return true, true //consensus + once
				} else {
					return true, false //consensus but not once
				}
			} else {
				return false, false //not consensus
			}
		} else {
			return true, false //consensus but not once
		}
	}
	return false, false //Paxos round not found
}

//PROPOSER-------------------------------------------------------------------------------------------------------------
func (p *Proposer) New() *Proposer {
	p.promiseDS = make(map[int]chan *types.PaxosPromise)
	p.acceptDS = make(map[int]chan types.Block)
	p.phase = 0
	p.consensusChan = make(chan bool)
	p.consensusTlc = false
	return p
}

//NOT USED
func (p *Proposer) QueryPromises(ID int) (*types.PaxosPromise, bool) {
	p.mux.RLock()
	defer p.mux.RUnlock()
	if promiseChan, found := p.promiseDS[ID]; found {
		promise := <-promiseChan
		return promise, found
	} else {
		return nil, found
	}
}
func (p *Proposer) SignalPhase(phase int) {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.phase = phase
}
func (p *Proposer) GetPhase() int {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.phase
}
func (p *Proposer) SignalConsensusTlc() {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.consensusTlc = true
}
func (p *Proposer) CheckConsensusTlc() bool {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.consensusTlc
}

//ACCEPTOR-----------------------------------------------------------------------------------
func (a *Acceptor) New() *Acceptor {
	a.highestIDa = -1
	a.highestIDp = -1
	a.highestIdaValue = types.Block{}
	return a
}

//UTILITIES--------------------------------------------------------------------------
func (msg *RumorMessage) isPaxos() bool {
	if msg.Extra != nil {
		return true
	}
	//if there's a TLC we want to treat it as a rumor to be stored
	return false
}
