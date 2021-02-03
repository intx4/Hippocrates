package gossip

import (
	"encoding/hex"
	"sync"
	//HW2 imports
)

type PendingRequest struct {
	request   *DataRequest
	replychan chan *DataReply
}
type PendingRequests struct {
	mux      sync.RWMutex
	requests []*PendingRequest
}

func (pr *PendingRequest) New(request *DataRequest) *PendingRequest {
	p := new(PendingRequest)
	p.request = request
	p.replychan = make(chan *DataReply)
	return p
}
func (pr *PendingRequests) New() *PendingRequests {
	p := new(PendingRequests)
	p.requests = make([]*PendingRequest, 0)
	return p
}

func (pr *PendingRequests) AddRequest(request *DataRequest) (*PendingRequest, int) {
	pr.mux.Lock()
	defer pr.mux.Unlock()
	pr.requests = append(pr.requests, new(PendingRequest).New(request))
	return pr.requests[len(pr.requests)-1], len(pr.requests) - 1
}
func (pr *PendingRequests) RemoveRequest(ticket int) {
	pr.mux.Lock()
	defer pr.mux.Unlock()
	newRequests := make([]*PendingRequest, len(pr.requests)-1)
	k := 0
	for i := 0; i < len(pr.requests) && len(newRequests) > 0; {
		if i != ticket {
			newRequests[k] = pr.requests[i]
			k++
			i++
		} else {
			i++
		}
	}
	pr.requests = newRequests
}
func (pr *PendingRequests) SendToChannel(reply *DataReply) {
	//ack the right request when receiving a reply
	pr.mux.Lock()
	defer pr.mux.Unlock()
	for _, r := range pr.requests {
		if hex.EncodeToString(r.request.HashValue) == hex.EncodeToString(reply.HashValue) && r.request.Destination == reply.Origin {
			r.replychan <- reply
		}
	}
}
