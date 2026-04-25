package main

import (
	"total-ordered-replicated-memcached/multicast"
	"time"
	"fmt"
	"flag"
	"strings"
	"sync"
	"encoding/csv"
	"encoding/gob"
	"math/rand"
	"net"
	"os"
)

type Result struct {
	ReqType string
	Target  string
	Success bool
	Latency time.Duration
}

func main() {

	targetsGroup := flag.String("targets", "localhost:8001,localhost:8002,localhost:8003", "Server addresses, separated by commas")
	targets := strings.Split(*targetsGroup, ",")
	ratio := flag.Float64("ratio", 0.1, "Probability of a SET")
	userCount := flag.Int("user_count", 10, "Number of users")
	duration := flag.Duration("duration", 60*time.Second, "Simulation duration")
	output := flag.String("output", "results.csv", "Output csv location")
	flag.Parse()

	fmt.Println("...Pre-populating database...")
	for i := 0; i < 100; i++ {
		executeReq(targets[0], "SET", fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i), "pre-populate", 0)
	}

	fmt.Printf("...Starting simulation for %d users...\n", *userCount)
	results := make(chan Result, 100000)
	var wg sync.WaitGroup
	start := time.Now()
	stop := time.After(*duration)

	for i := 0; i < *userCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client_%d", id)
			var reqID int64 = 0
			target := targets[id % len(targets)] 
			for {
				select {
				case <-stop:
					return
				default:
					reqID++
					key := fmt.Sprintf("key_%d", rand.Intn(100))
					var reqType string
					var val string
					if rand.Float64() >= *ratio {
						reqType = "GET"
						val = ""
					} else {
						reqType = "SET"
						val = fmt.Sprintf("value_%d", rand.Intn(10000)) 
					}
					result := executeReq(target, reqType, key, val, clientID, reqID)
					results <- result
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	saveResults(*output, results)
	fmt.Printf("---Done, total time: %v---\n", time.Since(start))

}

func executeReq(target string, reqType string, key string, value string, clientID string, reqID int64) Result {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return Result{reqType, target, false, time.Since(start)}
	}
	defer conn.Close()
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	var packetOut multicast.Packet
	if reqType == "GET" {
		packetOut = multicast.Packet{MsgType: multicast.ClientGetRequest, Msg: multicast.GetRequest{Key: key, ClientID: clientID, RequestID: reqID}}
	} else {
		packetOut = multicast.Packet{MsgType: multicast.ClientSetRequest, Msg: multicast.SetRequest{Key: key, Value: value, ClientID: clientID, RequestID: reqID}}
	}
	err := encoder.Encode(packetOut)
	if err != nil {
		return Result{reqType, target, false, time.Since(start)}
	}
	var packetIn multicast.Packet
	err := decoder.Decode(&packetIn)
	if err != nil {
		return Result{reqType, target, false, time.Since(start)}
	}
	success := false
	if reqType == "SET" {
		msg, ok := packetIn.Msg.(multicast.SetResponse)
		if ok {
			success = msg.Success
		}
	} else {
		msg, ok := packetIn.Msg.(multicast.GetResponse)
		if ok {
			success = msg.Success
		}
	}
	return Result{reqType, target, success, time.Since(start)}
}

func saveResults(fileLocation string, results <-chan Result) {
	file, err := os.Create(fileLocation)
	if err != nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"ReqType", "Target", "Success", "Latency_us"})
	for result := range results {
		writer.Write([]string{result.ReqType, result.Target, fmt.Sprintf("%t", result.Success), fmt.Sprintf("%d", result.Latency.Microseconds())})
	}
}
