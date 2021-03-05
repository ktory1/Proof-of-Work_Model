// Package powlib provides an API which is a wrapper around RPC calls to the
// coordinator.
package powlib

import (
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net/rpc"
	"sync"
)

type PowlibMiningBegin struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type PowlibMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type PowlibSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type PowlibMiningComplete struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

// MineResult contains the result of a mining request.
type MineResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan MineResult

// CloseChannel is used for notifying powlib goroutines of powlib Close event
type CloseChannel chan struct{}

// POW struct represents an instance of the powlib.
type POW struct {
	coordinator *rpc.Client
	notifyCh    NotifyChannel
	closeCh     CloseChannel
	closeWg     *sync.WaitGroup
}

func NewPOW() *POW {
	return &POW{
		coordinator: nil,
		notifyCh:    nil,
		closeWg:     nil,
	}
}

// Initialize Initializes the instance of POW to use for connecting to the coordinator,
// and the coordinators IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by powlib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *POW) Initialize(coordAddr string, chCapacity uint) (NotifyChannel, error) {
	// connect to Coordinator
	log.Printf("dailing coordinator at %s", coordAddr)
	coordinator, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing coordinator: %s", err)
	}
	d.coordinator = coordinator

	// create notify channel with given capacity
	d.notifyCh = make(NotifyChannel, chCapacity)
	d.closeCh = make(CloseChannel, chCapacity)

	var wg sync.WaitGroup
	d.closeWg = &wg

	return d.notifyCh, nil
}

// Mine is a non-blocking request from the client to the system solve a proof
// of work puzzle. The arguments have identical meaning as in A2. In case
// there is an underlying issue (for example, the coordinator cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking, and the solution to the proof of work
// puzzle must be delivered asynchronously to the client via the notify-channel
// channel returned in the Initialize call.
func (d *POW) Mine(tracer *tracing.Tracer, nonce []uint8, numTrailingZeros uint) error {
	tracer.RecordAction(PowlibMiningBegin{
		Nonce:            nonce,
		NumTrailingZeros: numTrailingZeros,
	})
	d.closeWg.Add(1)
	go d.callMine(tracer, nonce, numTrailingZeros)
	return nil
}

// Close Stops the POW instance from communicating with the coordinator and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *POW) Close() error {

	// notify all callMine goroutines of close
	d.closeCh <- struct{}{}
	d.closeWg.Wait()
	log.Println("All callMine routine existed")

	// close coordinator connection
	err := d.coordinator.Close()
	if err != nil {
		return err
	}
	d.coordinator = nil
	log.Println("Closed Coordinator connection")

	return nil
}

func (d *POW) callMine(tracer *tracing.Tracer, nonce []uint8, numTrailingZeros uint) {
	defer func() {
		log.Printf("callMine done")
		d.closeWg.Done()
	}()

	args := PowlibMine{
		Nonce:            nonce,
		NumTrailingZeros: numTrailingZeros,
	}
	tracer.RecordAction(args)

	result := MineResult{}
	call := d.coordinator.Go("CoordRPCHandler.Mine", args, &result, nil)
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				tracer.RecordAction(PowlibSuccess{
					Nonce:            result.Nonce,
					NumTrailingZeros: result.NumTrailingZeros,
					Secret:           result.Secret,
				})
				tracer.RecordAction(PowlibMiningComplete{
					Nonce:            result.Nonce,
					NumTrailingZeros: result.NumTrailingZeros,
					Secret:           result.Secret,
				})
				d.notifyCh <- result
			}
			return
		case <-d.closeCh:
			log.Printf("cancel callMine")
			d.closeCh <- struct{}{}
			return
		}
	}
}
