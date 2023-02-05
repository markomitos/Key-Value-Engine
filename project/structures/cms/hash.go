package cms

import (
	"crypto/md5"
	"encoding/binary"
	"time"
)

//cms hashwithseed
type CmsHashWithSeed struct {
	Seed []byte
}

func (h CmsHashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CmsCreateHashFunctions(k uint32) []CmsHashWithSeed {
	h := make([]CmsHashWithSeed, k)
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := CmsHashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
