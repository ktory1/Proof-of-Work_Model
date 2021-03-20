package distpow

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type WorkerAddr string

type WorkerClient struct {
	addr       WorkerAddr
	client     *rpc.Client
	workerByte uint8
}

type CoordinatorConfig struct {
	ClientAPIListenAddr string
	WorkerAPIListenAddr string
	Workers             []WorkerAddr
	TracerServerAddr    string
	TracerSecret        []byte
}

type CoordinatorMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CoordinatorWorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorWorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           string
}

type CoordinatorWorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           string
}

type Coordinator struct {
	config  CoordinatorConfig
	tracer  *tracing.Tracer
	workers []*WorkerClient
}

/****** RPC structs ******/
type MineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CoordMineArgs struct {
	MineArgs MineArgs
	Token    tracing.TracingToken
}

type CoordMineResponse struct {
	MineRes  CoordinatorSuccess
	RetToken tracing.TracingToken
}

type CoordResultArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           string
	RetToken         tracing.TracingToken
}

type ResultChan chan CoordResultArgs

type CoordRPCHandler struct {
	tracer     *tracing.Tracer
	workers    []*WorkerClient
	workerBits uint
	mineTasks  CoordinatorMineTasks
}

type CoordinatorMineTasks struct {
	mu    sync.Mutex
	tasks map[string]ResultChan
}

func NewCoordinator(config CoordinatorConfig) *Coordinator {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "coordinator",
		Secret:         config.TracerSecret,
	})

	workerClients := make([]*WorkerClient, len(config.Workers))
	for i, addr := range config.Workers {
		workerClients[i] = &WorkerClient{
			addr:       addr,
			client:     nil,
			workerByte: uint8(i),
		}
	}

	return &Coordinator{
		config:  config,
		tracer:  tracer,
		workers: workerClients,
	}
}

// Mine is a blocking RPC from powlib instructing the Coordinator to solve a specific pow instance
func (c *CoordRPCHandler) Mine(args CoordMineArgs, reply *CoordMineResponse) error {
	trace := c.tracer.ReceiveToken(args.Token)
	trace.RecordAction(CoordinatorMine{
		NumTrailingZeros: args.MineArgs.NumTrailingZeros,
		Nonce:            args.MineArgs.Nonce,
	})

	// initialize and connect to workers (if not already connected)
	for err := initializeWorkers(c.workers); err != nil; {
		log.Println(err)
		err = initializeWorkers(c.workers)
	}

	workerCount := len(c.workers)

	resultChan := make(chan CoordResultArgs, workerCount)
	c.mineTasks.set(args.MineArgs.Nonce, args.MineArgs.NumTrailingZeros, resultChan)

	for _, w := range c.workers {
		args := WorkerMineArgs{
			Nonce:            args.MineArgs.Nonce,
			NumTrailingZeros: args.MineArgs.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			WorkerBits:       c.workerBits,
			Token:            trace.GenerateToken(),
		}

		reply := WorkerReply{}

		trace.RecordAction(CoordinatorWorkerMine{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
		})

		err := w.client.Call("WorkerRPCHandler.Mine", args, &reply)
		if err != nil {
			return err
		}
	}

	// wait for at least one result
	result := <-resultChan
	// sanity check
	if result.Secret == "" {
		log.Fatalf("First worker result appears to be cancellation ACK, from workerByte = %d", result.WorkerByte)
	}

	// after receiving one result, cancel all workers unconditionally.
	// the cancellation takes place of an ACK for any workers sending results.
	for _, w := range c.workers {
		if w.workerByte != result.WorkerByte {
			trace.RecordAction(CoordinatorWorkerCancel{
				Nonce:            args.MineArgs.Nonce,
				NumTrailingZeros: args.MineArgs.NumTrailingZeros,
				WorkerByte:       w.workerByte,
			})
		}
		args := WorkerCancelArgs{
			Nonce:            args.MineArgs.Nonce,
			NumTrailingZeros: args.MineArgs.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			Token:            trace.GenerateToken(),
		}
		err := w.client.Call("WorkerRPCHandler.Found", args, &struct{}{})
		if err != nil {
			return err
		}
	}

	log.Printf("Waiting for %d acks from workers, then we are done", workerCount)

	// wait for all all workers to send back cancel ACK, ignoring results (receiving them is logged, but they have no further use here)
	// we asked all workers to cancel, so we should get exactly workerCount ACKs.
	workerAcksReceived := 0
	for workerAcksReceived < workerCount {
		ack := <-resultChan
		if ack.Secret == "" {
			log.Printf("Counting toward acks: %v", ack)
			workerAcksReceived += 1
		} else {
			log.Printf("Dropping extra result: %v", ack)
		}
	}

	// delete completed mine task from map
	c.mineTasks.delete(args.MineArgs.Nonce, args.MineArgs.NumTrailingZeros)

	reply.MineRes.NumTrailingZeros = result.NumTrailingZeros
	reply.MineRes.Nonce = result.Nonce
	reply.MineRes.Secret = result.Secret

	trace.RecordAction(reply.MineRes)

	reply.RetToken = trace.GenerateToken()
	return nil
}

// Result is a non-blocking RPC from the worker that sends the solution to some previous pow instance assignment
// back to the Coordinator
func (c *CoordRPCHandler) Result(args CoordResultArgs, reply *struct{}) error {
	trace := c.tracer.ReceiveToken(args.RetToken)
	if args.Secret != "" {
		trace.RecordAction(CoordinatorWorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           args.Secret,
		})
	} else {
		log.Printf("Received worker cancel ack: %v", args)
	}
	c.mineTasks.get(args.Nonce, args.NumTrailingZeros) <- args
	return nil
}

func (c *Coordinator) InitializeRPCs() error {
	handler := &CoordRPCHandler{
		tracer:     c.tracer,
		workers:    c.workers,
		workerBits: uint(math.Log2(float64(len(c.workers)))),
		mineTasks: CoordinatorMineTasks{
			tasks: make(map[string]ResultChan),
		},
	}
	server := rpc.NewServer()
	err := server.Register(handler) // publish Coordinator<->worker procs
	if err != nil {
		return fmt.Errorf("format of Coordinator RPCs aren't correct: %s", err)
	}

	workerListener, e := net.Listen("tcp", c.config.WorkerAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.WorkerAPIListenAddr, e)
	}

	clientListener, e := net.Listen("tcp", c.config.ClientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.ClientAPIListenAddr, e)
	}

	go server.Accept(workerListener)
	server.Accept(clientListener)

	return nil
}

func initializeWorkers(workers []*WorkerClient) error {
	for _, w := range workers {
		if w.client == nil {
			client, err := rpc.Dial("tcp", string(w.addr))
			if err != nil {
				log.Printf("Waiting for worker %d", w.workerByte)
				return fmt.Errorf("failed to dial worker: %s", err)
			}
			w.client = client
		}
	}
	return nil
}

func (t *CoordinatorMineTasks) get(nonce []uint8, numTrailingZeros uint) ResultChan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)]
}

func (t *CoordinatorMineTasks) set(nonce []uint8, numTrailingZeros uint, val ResultChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)] = val
	log.Printf("New task added: %v\n", t.tasks)
}

func (t *CoordinatorMineTasks) delete(nonce []uint8, numTrailingZeros uint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateCoordTaskKey(nonce, numTrailingZeros))
	log.Printf("Task deleted: %v\n", t.tasks)
}

func generateCoordTaskKey(nonce []uint8, numTrailingZeros uint) string {
	return fmt.Sprintf("%s|%d", hex.EncodeToString(nonce), numTrailingZeros)
}
