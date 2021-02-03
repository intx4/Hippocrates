package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5000", "ip:port of a DHT node")
	numFingers := flag.Int("numFingers", 1, "max number of fingers to draw")
	flag.Parse()

	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()

	nodes := make(map[string]*cgraph.Node)
	visited := make(map[string]struct{})

	curr := *addr
	nodes[*addr] = createNode(graph, curr)
	for stop := false; !stop; _, stop = visited[curr] {
		visited[curr] = struct{}{}
		fmt.Println(curr)

		fingers, err := RPCGetFingers(curr)
		if err != nil {
			log.Fatal(err)
		}

		for i, finger := range *fingers {
			if i >= *numFingers {
				break
			}
			_, ok := nodes[finger]
			if !ok {
				nodes[finger] = createNode(graph, finger)
			}
			fmt.Println(curr, i, "->", finger)
			_, err = graph.CreateEdge("", nodes[curr], nodes[finger])
			if err != nil {
				log.Fatal(err)
			}
		}

		curr = (*fingers)[0]
	}

	g.SetLayout(graphviz.CIRCO)
	if err := g.RenderFilename(graph, graphviz.PNG, "dht.png"); err != nil {
		log.Fatal(err)
	}
}

func createNode(graph *cgraph.Graph, name string) *cgraph.Node {
	node, err := graph.CreateNode(name)
	if err != nil {
		log.Fatal(err)
	}
	return node
}
