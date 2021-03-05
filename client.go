package distpow

import (
	"errors"
	"example.org/cpsc416/a2/powlib"
	"github.com/DistributedClocks/tracing"
)

const ChCapacity = 10

type ClientConfig struct {
	ClientID         string
	CoordAddr        string
	TracerServerAddr string
	TracerSecret     []byte
}

type Client struct {
	NotifyChannel powlib.NotifyChannel
	id            string
	coordAddr     string
	pow           *powlib.POW
	tracer        *tracing.Tracer
	initialized   bool
	tracerConfig  tracing.TracerConfig
}

func NewClient(config ClientConfig, pow *powlib.POW) *Client {
	tracerConfig := tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.ClientID,
		Secret:         config.TracerSecret,
	}
	client := &Client{
		id:           config.ClientID,
		coordAddr:    config.CoordAddr,
		pow:          pow,
		tracerConfig: tracerConfig,
		initialized:  false,
	}
	return client
}

func (c *Client) Initialize() error {
	if c.initialized {
		return errors.New("client has been initialized before")
	}
	ch, err := c.pow.Initialize(c.coordAddr, ChCapacity)
	c.tracer = tracing.NewTracer(c.tracerConfig)
	c.NotifyChannel = ch
	c.initialized = true
	return err
}

func (c *Client) Mine(nonce []uint8, numTrailingZeros uint) error {
	return c.pow.Mine(c.tracer, nonce, numTrailingZeros)
}

func (c *Client) Close() error {
	if err := c.tracer.Close(); err != nil {
		return err
	}
	if err := c.pow.Close(); err != nil {
		return err
	}
	c.initialized = false
	return nil
}
