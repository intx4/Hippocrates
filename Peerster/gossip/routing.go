package gossip

import (
	"fmt"
	"sync"

	"go.dedis.ch/onet/v3/log"
)

// Routes keeps track of routing information
type Routes struct {
	routes map[string]*RouteStruct
	sync.Mutex
}

func (r *Routes) newRumor(msg *RumorMessage, from string) bool {
	r.Lock()
	defer r.Unlock()

	isRouteRumor := msg.isRouteRumor()
	if route, in := r.routes[msg.Origin]; in {
		if msg.ID > route.LastID {
			route.LastID = msg.ID
			route.NextHop = from
			if !isRouteRumor {
				log.Lvlf1("DSDV %s %s\n", msg.Origin, from)
			}
			return true
		}
	} else {
		r.routes[msg.Origin] = &RouteStruct{from, msg.ID}
		if !isRouteRumor {
			log.Lvlf1("DSDV %s %s\n", msg.Origin, from)
		}
		return true
	}
	return false
}

func (r *Routes) nextHop(dst string) (string, bool) {
	r.Lock()
	defer r.Unlock()
	if route, ok := r.routes[dst]; ok {
		return route.NextHop, ok
	} else {
		return "", false
	}
}

func (r *Routes) getRoutingTable() map[string]*RouteStruct {
	r.Lock()
	defer r.Unlock()
	return r.routes
	// we shouldn't receive our own messages ! but leave this here as a reminder
	/*copy := make(map[string]*RouteStruct)
	for dst, route := range r.routes {
		if dst != myName {
			copy[dst] = route
		}
	}
	return copy*/
}

func (r *Routes) getDirectNodes() []string {
	r.Lock()
	defer r.Unlock()
	nodes := make([]string, len(r.routes))
	i := 0
	for route := range r.routes {
		nodes[i] = route
		i++
	}
	return nodes
}

func (g *Gossiper) routingCallback() {
	fmt.Println(g.name, " sending route msg")
	g.AddMessage("")
}

func (msg *RumorMessage) isRouteRumor() bool {
	return msg.Text == ""
}

//HW3--------------------------------------------------------
func (r *Routes) AddRoute(peerName, nextHop string) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	if _, ok := r.routes[peerName]; !ok {
		r.routes[peerName] = &RouteStruct{NextHop: nextHop, LastID: 0}
	}
}
