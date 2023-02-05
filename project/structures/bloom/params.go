package bloom

import "math"

func CalculateM(expectedElements uint32, falsePositiveRate float64) uint32 {
	return uint32(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements uint32, m uint32) uint32 {
	return uint32(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}
