// ========== CS-438 HW0 Skeleton ===========
// *** Do not change this file ***

// This file should be the entering point to your program.
// Here, we only parse the input and start the logic implemented
// in other files.
package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/hw3/gossip"
)

const defaultGossipAddr = "127.0.0.1:33000" // IP address:port number for gossiping
const defaultName = "peerXYZ"               // Give a unique default name
const defaultPaxosRetry = 3                 // in seconds

var (
	// defaultLevel can be changed to set the desired level of the logger
	defaultLevel = zerolog.WarnLevel

	// logout is the logger configuration
	logout = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	// Logger is a globally available logger.
	Logger = zerolog.New(logout).Level(defaultLevel).
		With().Timestamp().Logger().
		With().Caller().Logger()
)

/*TEST DKG 3:
./hw3 --role c --name C1 --gossipAddr 127.0.0.1:5000 --dhtAddr 127.0.0.1:4000 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C2 --gossipAddr 127.0.0.1:5001 --dhtAddr 127.0.0.1:4001 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C3 --gossipAddr 127.0.0.1:5002 --dhtAddr 127.0.0.1:4002 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role d --name Dr.Cox --doctorKey hibarbie --gossipAddr 127.0.0.1:5003 --dhtAddr 127.0.0.1:4003 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role p --name Dorian --gossipAddr 127.0.0.1:5004 --dhtAddr 127.0.0.1:4004 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4001
*/

/*TEST DKG 5:
./hw3 --role c --name C1 --gossipAddr 127.0.0.1:5000 --dhtAddr 127.0.0.1:4000 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C2 --gossipAddr 127.0.0.1:5001 --dhtAddr 127.0.0.1:4001 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C3 --gossipAddr 127.0.0.1:5002 --dhtAddr 127.0.0.1:4002 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C4 --gossipAddr 127.0.0.1:5003 --dhtAddr 127.0.0.1:4003 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C5 --gossipAddr 127.0.0.1:5004 --dhtAddr 127.0.0.1:4004 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004 --dhtJoinAddr 127.0.0.1:4000
*/

/*TEST DKG 7:
./hw3 --role c --name C1 --gossipAddr 127.0.0.1:5000 --dhtAddr 127.0.0.1:4000 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C2 --gossipAddr 127.0.0.1:5001 --dhtAddr 127.0.0.1:4001 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C3 --gossipAddr 127.0.0.1:5002 --dhtAddr 127.0.0.1:4002 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C4 --gossipAddr 127.0.0.1:5003 --dhtAddr 127.0.0.1:4003 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C5 --gossipAddr 127.0.0.1:5004 --dhtAddr 127.0.0.1:4004 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C6 --gossipAddr 127.0.0.1:5005 --dhtAddr 127.0.0.1:4005 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C7 --gossipAddr 127.0.0.1:5006 --dhtAddr 127.0.0.1:4006 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006 --dhtJoinAddr 127.0.0.1:4001
*/

/*TEST DKG 9:
./hw3 --role c --name C1 --gossipAddr 127.0.0.1:5000 --dhtAddr 127.0.0.1:4000 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C2 --gossipAddr 127.0.0.1:5001 --dhtAddr 127.0.0.1:4001 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C3 --gossipAddr 127.0.0.1:5002 --dhtAddr 127.0.0.1:4002 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C4 --gossipAddr 127.0.0.1:5003 --dhtAddr 127.0.0.1:4003 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C5 --gossipAddr 127.0.0.1:5004 --dhtAddr 127.0.0.1:4004 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C6 --gossipAddr 127.0.0.1:5005 --dhtAddr 127.0.0.1:4005 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C7 --gossipAddr 127.0.0.1:5006 --dhtAddr 127.0.0.1:4006 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C8 --gossipAddr 127.0.0.1:5007 --dhtAddr 127.0.0.1:4007 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C9 --gossipAddr 127.0.0.1:5008 --dhtAddr 127.0.0.1:4008 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008 --dhtJoinAddr 127.0.0.1:4001
*/

/*TEST DKG 13:
./hw3 --role c --name C1 --gossipAddr 127.0.0.1:5000 --dhtAddr 127.0.0.1:4000 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C2 --gossipAddr 127.0.0.1:5001 --dhtAddr 127.0.0.1:4001 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C3 --gossipAddr 127.0.0.1:5002 --dhtAddr 127.0.0.1:4002 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C4 --gossipAddr 127.0.0.1:5003 --dhtAddr 127.0.0.1:4003 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C5 --gossipAddr 127.0.0.1:5004 --dhtAddr 127.0.0.1:4004 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C6 --gossipAddr 127.0.0.1:5005 --dhtAddr 127.0.0.1:4005 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C7 --gossipAddr 127.0.0.1:5006 --dhtAddr 127.0.0.1:4006 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C8 --gossipAddr 127.0.0.1:5007 --dhtAddr 127.0.0.1:4007 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name C9 --gossipAddr 127.0.0.1:5008 --dhtAddr 127.0.0.1:4008 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name CA --gossipAddr 127.0.0.1:5009 --dhtAddr 127.0.0.1:4009 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name CB --gossipAddr 127.0.0.1:5010 --dhtAddr 127.0.0.1:4010 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name CC --gossipAddr 127.0.0.1:5011 --dhtAddr 127.0.0.1:4011 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role c --name CD --gossipAddr 127.0.0.1:5012 --dhtAddr 127.0.0.1:4012 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005,127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008,127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011,127.0.0.1:5012 --dhtJoinAddr 127.0.0.1:4001
*/
/*
RESULTS:
3 -> 6s
5 -> 10s
7 -> 15s
9 -> 20s
13 > 29s
*/
func main() {

	gossipAddr := flag.String("gossipAddr", defaultGossipAddr, "ip:port for gossip communication with peers")
	ownName := flag.String("name", defaultName, "identifier used in the chat")
	peers := flag.String("peers", "", "peer addresses used for bootstrap")
	rootSharedData := flag.String("sharedir", "_SharedData/", "the directory to store data about indexed files")
	rootDownloadedFiles := flag.String("downdir", "_Downloads/", "the directory to store downloaded and reconstructed files")
	doctorKey := flag.String("doctorKey", "", "secret of doctor")
	role := flag.String("role", "p", "Role of this 4 62 244 28 162 180 157 38 39 110 253 91 167 55 244 179 215 35 50 61 163 243 48 183of the doctor (only needed if role is doctor)")
	cothorityAddr := flag.String("cothorityAddr", "", "Addresses of the cothorities (separated by ',')")
	dhtAddr := flag.String("dhtAddr", "", "Address to use for the DHT")
	dhtJoinAddr := flag.String("dhtJoinAddr", "", "Address to use to join the DHT")

	flag.Parse()

	if *dhtAddr == "" {
		fmt.Println("Please specify a DHT address")
		return
	}

	if *role != "p" && *role != "c" && *role != "d" {
		Logger.Error().Msg("invalid role")
		return
	}

	if *role == "d" && *doctorKey == "" {
		Logger.Error().Msg("please specify a doctor key")
		return
	}

	gossipAddress := *gossipAddr
	bootstrapAddr := strings.Split(*peers, ",")

	CNodes := strings.Split(*cothorityAddr, ",")

	fac := gossip.GetFactory()
	g, err := fac.New(gossipAddress, *ownName,
		*rootSharedData, *rootDownloadedFiles, *role, *doctorKey, CNodes, *dhtJoinAddr, *dhtAddr)

	if err != nil {
		panic(err)
	}

	if len(bootstrapAddr) > 0 && bootstrapAddr[0] != "" {
		fmt.Println(strconv.Itoa(len(bootstrapAddr)))
		g.AddAddresses(bootstrapAddr...)
	}

	port := strings.Split(gossipAddress, ":")[1]

	ready := make(chan struct{})
	go g.Run(ready)
	<-ready
	//Automate Cothority start
	if *role == "c" {
		fmt.Println("Starting cothority...")
		g.StartCothority()
	}
	prompt := *ownName + ":" + port + *role + "> "
	//fmt.Println(`Type "help" to learn about the available commands`)
	for {
		fmt.Printf(prompt)
		input, err := getInput()
		if err != nil {
			fmt.Println("There was an error with your command")
			continue
		}
		switch input[0] {
		case "":
			continue
		case "i":
			fmt.Println("IP:", gossipAddress)
			fmt.Println("Name:", *ownName)
		case "q":
			fmt.Println("Stopping...")
			g.Stop()
			fmt.Println("Bye!")
			os.Exit(0)
		case "p":
			if *role != "p" {
				fmt.Println("Only patients can publish files")
			} else if len(input) != 2 {
				fmt.Println("Usage: p FILE")
			} else {
				g.PublishFile(input[1])
			}
		case "r":
			if *role != "d" {
				fmt.Println("Only doctors can request files")
			} else if len(input) != 3 {
				fmt.Println("Usage: r USER FILE")
			} else {
				g.RequestFile(input[1], input[2])
			}
		case "sc":
			if *role == "c" {
				fmt.Println("Starting cothority...")
				g.StartCothority()
			} else {
				fmt.Println("Only for cothority nodes!")
			}
		case "b":
			if *role == "c" {
				last, bc := g.GetBlocks()
				fmt.Printf("%2s|%-10s|%-14s|%-40s|%-19s\n", " #", " Doctor", " IP", " File", " Time")
				fmt.Println("=========================================================================================")
				for last != "" {
					b, _ := bc[last]
					acc := b.Access
					t := time.Unix(acc.Time, 0).Format("2006-01-02 15:04:05")
					fmt.Printf("%2d|%-10s|%-14s|%-40s|%-19s\n", b.BlockNumber, acc.User, acc.Address, acc.File, t)
					last = hex.EncodeToString(b.PreviousHash)
					if b.BlockNumber == 0 {
						break
					}
				}
			} else {
				fmt.Println("Only for cothority nodes!")
			}
		case "show":
			for key, value := range *g.GetDHT() {
				fmt.Printf("Have key %v with value: %v\n", key, value)
			}
		default:
			fmt.Println("Command not found.")
		}
		fmt.Println()
	}
}

func getInput() ([]string, error) {
	b := bufio.NewReader(os.Stdin)
	line, err := b.ReadString('\n')
	if err != nil {
		return []string{}, err
	}
	return strings.Split(strings.TrimSpace(line), " "), nil
}
