package gossip

import (
	"time"
)

func startTicker(timeInSeconds int, f func()) chan bool {
	done := make(chan bool, 1)
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(timeInSeconds))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				f()
			case <-done:
				return
			}
		}
	}()
	return done
}

func (g *Gossiper) antiEntropyCallback() {
	status := g.vc.getStatus()
	msg := StatusPacket{status}
	pkt := GossipPacket{Status: &msg}
	//g.sendToRandom(pkt)
	g.sendToRandomAmong(pkt, g.GetCn().ReturnParticipantsList())
}
