// A simple utility for generating random port numbers in this assignment's config files.
//
// When run from the root directory, this utility will overwrite the addresses in
// config/*.json with addresses of the format :*, referring to a pseudo-randomly selected
// local port (above 1024).
// This can be used during testing on shared servers, to (try and) avoid port collisions.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	distpow "example.org/cpsc416/a2"
	"github.com/DistributedClocks/tracing"
)

func genPort() int32 {
	return rand.Int31n(35535-1024) + 1024
}

func updateConfig(fileName string, emptyConfig interface{}, updateFn func()) {
	path := filepath.Join("config", fileName)
	fileRead, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer fileRead.Close()
	decoder := json.NewDecoder(fileRead)
	err = decoder.Decode(emptyConfig)
	if err != nil {
		log.Fatal(err)
	}
	updateFn()
	fileWrite, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	encoder := json.NewEncoder(fileWrite)
	encoder.SetIndent("", "\t")
	err = encoder.Encode(emptyConfig)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	workerAddrs := []distpow.WorkerAddr{}
	traceServerAddr := fmt.Sprintf(":%v", genPort())
	coordinatorClientListenAddr := fmt.Sprintf(":%v", genPort())
	coordinatorWorkerListenAddr := fmt.Sprintf(":%v", genPort())

	traceServerConfig := &tracing.TracingServerConfig{}
	updateConfig("tracing_server_config.json", traceServerConfig, func() {
		traceServerConfig.ServerBind = traceServerAddr
	})

	coordinatorConfig := &distpow.CoordinatorConfig{}
	updateConfig("coordinator_config.json", coordinatorConfig, func() {
		for i, _ := range coordinatorConfig.Workers {
			addr := distpow.WorkerAddr(fmt.Sprintf(":%v", genPort()))
			coordinatorConfig.Workers[i] = addr
			workerAddrs = append(workerAddrs, addr)
		}
		coordinatorConfig.TracerServerAddr = traceServerAddr
		coordinatorConfig.ClientAPIListenAddr = coordinatorClientListenAddr
		coordinatorConfig.WorkerAPIListenAddr = coordinatorWorkerListenAddr
	})

	clientConfig := &distpow.ClientConfig{}
	updateConfig("client_config.json", clientConfig, func() {
		clientConfig.TracerServerAddr = traceServerAddr
		clientConfig.CoordAddr = coordinatorClientListenAddr
	})

	workerConfig := &distpow.WorkerConfig{}
	updateConfig("worker_config.json", workerConfig, func() {
		workerConfig.TracerServerAddr = traceServerAddr
		workerConfig.CoordAddr = coordinatorWorkerListenAddr
		workerConfig.ListenAddr = "PASS VIA COMMAND-LINE"
	})
}
