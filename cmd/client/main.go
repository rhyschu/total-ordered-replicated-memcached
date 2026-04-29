// client main.go
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
	Key	    string
	Value   string
	Target  string
	Success bool
	Latency time.Duration
}

func main() {

	targetsGroup := flag.String("targets", "localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005", "Server addresses, separated by ','")
	ratio := flag.Float64("ratio", 0.1, "Probability of a SET (default: 0.1)")
	databaseSize := flag.Int("database", 100, "Database size (default: 100)")
	userCount := flag.Int("user", 20, "Number of users (default: 20)")
	duration := flag.Duration("duration", 10*time.Second, "Simulation duration (default: 10s)")
	output := flag.String("output", "data/csvs/results.csv", "Output csv location")
	flag.Parse()
	targets := strings.Split(*targetsGroup, ",")

	fmt.Println("...Pre-populating database...")
	conn, _ := net.Dial("tcp", targets[0])
    defer conn.Close()
    enc := gob.NewEncoder(conn)
    for i := 0; i < *databaseSize; i++ {
        pac := multicast.Packet{MsgType: multicast.ClientSetRequest, Msg: multicast.SetRequest{Key: fmt.Sprintf("key_%d", i), Value: fmt.Sprintf("value_%d", i), ClientID: "pre", RequestID: 0}}
        enc.Encode(pac)
    }

	fmt.Println("...Starting simulation...")
	results := make(chan Result, 100000)
	var waitGroup sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < *userCount; i++ {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			clientID := fmt.Sprintf("client_%d", id)
			firstTarget := targets[id % len(targets)]
			secondTarget := targets[(id + 1) % len(targets)]
			fConn, _ := net.Dial("tcp", firstTarget)
			sConn, _ := net.Dial("tcp", secondTarget)
			defer fConn.Close()
			defer sConn.Close()
			fEnc := gob.NewEncoder(fConn)
			fDec := gob.NewDecoder(fConn)
			sEnc := gob.NewEncoder(sConn)
			sDec := gob.NewDecoder(sConn)
			var reqID int64 = 0
			for {
				select {
				case <-stop:
					return
				default:
					reqID++
					key := fmt.Sprintf("key_%d", rand.Intn(*databaseSize))
					reqType := "GET"
					val := ""
					hedge := 5*time.Millisecond
					if rand.Float64() < *ratio {
						reqType = "SET"
						val = fmt.Sprintf("value_%d", rand.Intn(10000))
						hedge = 100*time.Millisecond
					}
					resultChan := make(chan Result, 2)
					go func() {
						resultChan <- execute(fEnc, fDec, firstTarget, reqType, key, val, clientID, reqID)
					}()
					var result Result
					select {
					case result = <-resultChan:
					case <-time.After(hedge):
						go func() {
							resultChan <- execute(sEnc, sDec, secondTarget, reqType, key, val, clientID, reqID)
						}()
						result = <-resultChan
					}
					results <- result
				}
			}
		}(i)
	}

	time.AfterFunc(*duration, func() {
		close(stop)
	})

	go func() {
		waitGroup.Wait()
		close(results)
	}()

	saveResults(*output, results)
	fmt.Println("---Done---")

}

func execute(enc *gob.Encoder, dec *gob.Decoder, target, reqType, key, value, clientID string, reqID int64) Result {
	start := time.Now()
	var packetOut multicast.Packet
	if reqType == "GET" {
		packetOut = multicast.Packet{MsgType: multicast.ClientGetRequest, Msg: multicast.GetRequest{Key: key, ClientID: clientID, RequestID: reqID}}
	} else {
		packetOut = multicast.Packet{MsgType: multicast.ClientSetRequest, Msg: multicast.SetRequest{Key: key, Value: value, ClientID: clientID, RequestID: reqID}}
	}
	if err := enc.Encode(packetOut); err != nil {
		return Result{reqType, key, value, target, false, time.Since(start)}
	}
	var packetIn multicast.Packet
	if err := dec.Decode(&packetIn); err != nil {
		return Result{reqType, key, value, target, false, time.Since(start)}
	}
	success := false
	getValue := value
	if reqType == "SET" {
		if msg, ok := packetIn.Msg.(multicast.SetResponse); ok {
			success = msg.Success
		}
	} else {
		if msg, ok := packetIn.Msg.(multicast.GetResponse); ok {
			success = msg.Success
			getValue = msg.Value
		}
	}
	return Result{reqType, key, getValue, target, success, time.Since(start)}
}

func prePopulate(target, key, value string) {
	conn, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer conn.Close()
	enc := gob.NewEncoder(conn)
	pac := multicast.Packet{
		MsgType: multicast.ClientSetRequest, 
		Msg: multicast.SetRequest{Key: key, Value: value, ClientID: "pre", RequestID: 0},
	}
	enc.Encode(pac)
}

func saveResults(fileLocation string, results <-chan Result) {
	file, err := os.Create(fileLocation)
	if err != nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"ReqType", "Key", "Value", "Target", "Success", "Latency_us"})
	for result := range results {
		writer.Write([]string{result.ReqType, result.Key, result.Value, result.Target, fmt.Sprintf("%t", result.Success), fmt.Sprintf("%d", result.Latency.Microseconds())})
	}
}
