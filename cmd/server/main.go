package main

import (
	"total-ordered-replicated-memcached/memcached"
	"total-ordered-replicated-memcached/multicast"
	"time"
	"fmt"
	"flag"
	"strings"
	"encoding/gob"
	"net"
	"io"
	"log"
)

func main() {

	id := flag.Int("id", 1, "Server ID")
	port := flag.String("port", "8001", "Port")
	mcID := flag.String("mc", "21211", "Memcached ID")
	peersGroup := flag.String("peers", "", "Peers port, separated by commas")
	peers := strings.Split(*peersGroup, ",")
	consistent := flag.Bool("consistent", true, "Use consistent protocol?")
	isSequencer := flag.Bool("sequencer", false, "Is this the initial sequencer?")
	batchSize := flag.Int("batch", 1, "Batch size")
	latency := flag.Duration("artificial_latency", 0, "Artificial network latency")
	flag.Parse()

	mcAddress := fmt.Sprintf("127.0.0.1:%s", *mcID)
	mcClient, err := memcached.NewClient(mcAddress)
	fmt.Printf("---Server %d connected to memcached %s---\n", *id, mcAddress)

	peerMap := make(map[int]string)
	curr := 0
	for i := 1; i <= len(peers) + 1; i++ {
		if i == *id {
			continue
		}
		if curr < len(peers) && peers[curr] != "" {
			peerMap[i] = peers[curr]
			curr++
		}
	}

	sendFunc := func(targetID int, pac multicast.Packet) {
		if *latency > 0 {
			time.Sleep(*latency)
		}
		address, ok := peerMap[targetID]
		if !ok {
			log.Printf("Unknown target ID %d", targetID)
			return
		}
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			log.Printf("Failed to connect to peer %d at %s: %v", targetID, address, err)
			return
		}
		defer conn.Close()
		encoder := gob.NewEncoder(conn)
		if encoder.Encode(pac) != nil {
			log.Printf("Failed to encode packet to peer %d: %v", targetID, err)
		}
	}

	var protocol multicast.ConsistencyProtocol
	peerIDs := make([]int, 0, len(peerMap))
	for pid := range peerMap {
		peerIDs = append(peerIDs, pid)
	}
	if *consistent {
		protocol = multicast.NewConsistent(mcClient, *id, peerIDs, sendFunc, *isSequencer, *batchSize)
		fmt.Printf("Server %d running in CONSISTENT mode (Sequencer: %t)\n", *id, *isSequencer)
	} else {
		protocol = multicast.NewInconsistent(mcClient, *id, peerIDs, sendFunc)
		fmt.Printf("Server %d running in INCONSISTENT mode\n", *id)
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}
	defer listener.Close()
	fmt.Printf("Server %d listening on port %s...\n", *id, *port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go handleConnection(conn, protocol)
	}

}

func handleConnection(conn net.Conn, protocol multicast.ConsistencyProtocol) {
	defer conn.Close()
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)
	for {
		var pac multicast.Packet
		err := decoder.Decode(&pac)
		if err != nil {
			if err != io.EOF {
				log.Printf("Decode error: %v", err)
			}
			return
		}
		switch pac.MsgType {
		case multicast.ClientGetRequest:
			req := pac.Msg.(multicast.GetRequest)
			packetIn := protocol.HandleClientGet(req)
			encoder.Encode(multicast.Packet{MsgType: multicast.ClientGetResponse, Msg: packetIn})
		case multicast.ClientSetRequest:
			req := pac.Msg.(multicast.SetRequest)
			packetIn := protocol.HandleClientSet(req)
			encoder.Encode(multicast.Packet{MsgType: multicast.ClientSetResponse, Msg: packetIn})
		default:
			err := protocol.HandleServerMsg(pac)
			if err != nil {
				log.Printf("Protocol error handling server msg: %v", err)
			}
		}
	}
}
