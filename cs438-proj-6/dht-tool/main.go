package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"test/project"
	"time"
)

var node *Node

type NodeCmd struct {
	Command *exec.Cmd
	Pipe    io.WriteCloser
}

var nodes map[int]*NodeCmd

var commands = map[string]func(args ...string) error{
	//"quit":   quitFn,
	"i": infoFn,
	"c": createFn,
	"p": putFn,
	"g": getFn,
	"j": joinFn,
	"s": showTableFn,
	"q": quit,
}

func infoFn(args ...string) error {
	node.PrintInfo()
	return nil
}

func createFn(args ...string) error {
	switch len(args) {
	case 1:
		node = NewNode("127.0.0.1:" + args[0])
	case 3:
		from, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return err
		}
		to, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return err
		}
		join, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		createNodes(int(from), int(to), int(join))
	}
	return nil
}

func joinFn(args ...string) error {
	if len(args) == 0 {
		node.Join("")
	} else {
		node.Join("127.0.0.1:" + args[0])
	}
	return nil
}

func createNodes(from, to, join int) {
	for i := from; i <= to; i++ {
		time.Sleep(10 * time.Second)
		cmd := exec.Command("./test")

		pipe, _ := cmd.StdinPipe()

		// outfile, err := os.Create(strconv.Itoa(i))
		// if err != nil {
		// 	log.Fatal(err)
		// }
		outfile := ioutil.Discard
		cmd.Stdout = outfile

		err := cmd.Start()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Fprintf(pipe, "c %d\nj %d\n", i, join)

		nodes[i] = &NodeCmd{
			Command: cmd,
			Pipe:    pipe,
		}
	}
}

func putFn(args ...string) error {
	i, err := strconv.Atoi(args[0])
	if err != nil {
		return err
	}
	node.Put(args[0], &project.DhtVal{Index: i, EntryHash: args[1]})
	return nil
}

func getFn(args ...string) error {
	dhtVal, err := node.Get(args[0])
	if err != nil {
		return err
	}
	fmt.Printf("Got value: %v %v\n", dhtVal)
	return nil
}

func showTableFn(args ...string) error {
	for key, value := range node.data {
		fmt.Printf("Have key %v with value: %v\n", key, value)
	}
	return nil
}

func quit(args ...string) error {
	if node != nil {
		node.Quit()
	}
	for _, cmd := range nodes {
		fmt.Fprint(cmd.Pipe, "q\n")
		cmd.Pipe.Close()
		cmd.Command.Wait()
	}
	os.Exit(0)
	return nil
}

func testStatistics(totalNode int, repeats int) error {
	node = NewNode("127.0.0.1:65300")
	node.Join("")
	createNodes(65301, 65301+totalNode-1, 65300)
	time.Sleep(60 * time.Second)
	var totalTime int64 = 0
	for i := 0; i < repeats; i++ {
		var succ string
		err := node.GetSuccessor(0, &succ)
		if err != nil {
			log.Fatal("Could not get successor")
		}

		start := time.Now()
		succ, err = RPCFindSuccessor(succ, node.id)
		if err != nil {
			log.Fatal("Could not find successor")
		}
		elapsed := time.Since(start)

		totalTime += elapsed.Microseconds()
	}
	fmt.Println("AVG", totalTime/int64(repeats))
	for _, cmd := range nodes {
		fmt.Fprint(cmd.Pipe, "q\n")
		cmd.Pipe.Close()
		cmd.Command.Wait()
	}
	node.Quit()
	os.Exit(0)
	return nil
}

func main() {

	nodes = make(map[int]*NodeCmd)

	totalNode := flag.Int("totalNode", 0, "help message for flag n")
	repeats := flag.Int("repeats", 0, "help message for flag n")

	flag.Parse()

	if *repeats != 0 {
		testStatistics(*totalNode, *repeats)
		return
	}

	b := bufio.NewReader(os.Stdin)
	prompt := "dht> "
	// Enter the repl
	fmt.Println(`Welcome to dht. Type "help" to learn about the available commands`)
	for {
		fmt.Printf(prompt)
		line, err := b.ReadString('\n')
		if err != nil {
			fmt.Println("There was an error with your command")
			continue
		}
		input := strings.Split(strings.TrimSpace(line), " ")
		if _, ok := commands[input[0]]; !ok {
			fmt.Println("Command not found.")
			continue
		}
		err = commands[input[0]](input[1:]...)
		if err != nil {
			fmt.Println(err)
		}
	}
}
