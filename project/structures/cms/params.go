package cms

import (
	"math"
)

func CmsCalculateM(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func CmsCalculateK(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}
