package distpow

type CacheAdd struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheRemove struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheHit struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheMiss struct {
	Nonce            []uint8
	NumTrailingZeros uint
}
