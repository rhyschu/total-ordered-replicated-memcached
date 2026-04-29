// server main.go
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
	"math/rand"
)

func main() {

	id := flag.Int("id", 1, "Server ID")
	port := flag.String("port", "8001", "Port")
	mcID := flag.String("mc", "21211", "Memcached ID")
	peersGroup := flag.String("peers", "", "Peers port, preceded by ':' and separated by ',")
	consistent := flag.Bool("consistent", true, "Use consistent protocol? (default: true)")
	isSequencer := flag.Bool("sequencer", false, "Is this the initial sequencer? (default: false)")
	batchSize := flag.Int("batch", 1, "Batch size (default: 1)")
	slow := flag.Bool("slow", false, "Is this an intentionally slow server? (default: false)")
	flag.Parse()
	peers := strings.Split(*peersGroup, ",")

	mcAddress := fmt.Sprintf("localhost:%s", *mcID)
	mcClient, err := memcached.NewClient(mcAddress)
	fmt.Printf("---Server %d connected to memcached %s---\n", *id, mcAddress)

	var protocol multicast.ConsistencyProtocol

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
		if *slow {
			time.Sleep(5*time.Millisecond + time.Duration(rand.Float64()*float64(10*time.Millisecond)))
		}
		address, ok := peerMap[targetID]
		if !ok {
			fmt.Printf("Unknown target ID %d\n", targetID)
			return
		}
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			fmt.Printf("Failed to connect to peer %d\n", targetID)
			if *consistent {
				go protocol.InitElection()
			}
			return
		}
		defer conn.Close()
		encoder := gob.NewEncoder(conn)
		if encoder.Encode(pac) != nil {
			fmt.Printf("Failed to encode packet to peer %d\n", targetID)
		}
	}

	peerIDs := make([]int, 0, len(peerMap))
	for pid := range peerMap {
		peerIDs = append(peerIDs, pid)
	}
	
	if *consistent {
		protocol = multicast.NewConsistent(mcClient, *id, peerIDs, sendFunc, *isSequencer, *batchSize)
	} else {
		protocol = multicast.NewInconsistent(mcClient, *id, peerIDs, sendFunc)
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Printf("Failed to connect to port %s\n", *port)
	}
	defer listener.Close()
	fmt.Printf("---Server %d is connected to port %s---\n", *id, *port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Accept error: %v\n", err)
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
				fmt.Printf("Decode error: %v\n", err)
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
				fmt.Printf("Server message error: %v\n", err)
			}
		}
	}
}
