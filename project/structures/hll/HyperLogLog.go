package hll

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"strconv"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M   uint64 // velicina niza elemenata
	P   uint8 // preciznost
	Reg []uint8
}

// Hash i pretvaranje u binaran oblik
func Hash(data string) string {
	h := fnv.New32a()
	h.Write([]byte(data))
	num := h.Sum32()

	//Dodajemo nule na pocetak da se dopune 32 bita
	str := fmt.Sprintf("%b", num)
	for len(str) < 32 {
		str = "0" + str
	}

	return str
}

// Konstruktor
func NewHLL(precision uint8) (*HLL, error) {
	hll := new(HLL)
	if precision < HLL_MIN_PRECISION && precision > HLL_MAX_PRECISION {
		return nil, errors.New("Preciznost mora biti izmedju 4 i 16")
	}
	hll.P = precision
	hll.M = uint64(math.Pow(2, float64(precision)))
	hll.Reg = make([]uint8, hll.M)
	return hll, nil
}

//Dodaje element u hll
func (hll *HLL) AddToHLL(elem string) {
	hashString := Hash(elem)
	// fmt.Println(hashString)
	bucketString := hashString[:hll.P]
	bucket, err := strconv.ParseInt(bucketString, 2, 64)
	if err != nil {
		log.Fatal(err)
	}

	zerosCount := 1
	for i := uint8(len(hashString)) - 1; i >= hll.P; i-- {
		if hashString[i] == '0' {
			zerosCount++
		} else {
			break
		}
	}

	if hll.Reg[bucket] < uint8(zerosCount) {
		hll.Reg[bucket] = uint8(zerosCount)
	}

}

// Vraca procenjeni broj elemenata 
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

// Pomocna funkcija koja racuna nule
func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

//Pretvara hll u niz bajtova
func HyperLogLogToBytes(hll *HLL) []byte {

	//upisujemo promenljive tipa uint32
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, hll.M)

	bytesP := make([]byte, 0)
	bytesP = append(bytesP, byte(hll.P))
	bytes = append(bytes, bytesP...)

	bytesReg := make([]byte, 0)
	for _, b:= range hll.Reg {
		bytesReg = append(bytesReg, byte(b))
	}
	bytes = append(bytes, bytesReg...)

	return bytes
}

//Pretvara niz bajtova u hll
func BytesToHyperLogLog(HllBytes []byte) *HLL {
	hll := new(HLL)
	reader := bytes.NewReader(HllBytes)
	bytes := make([]byte, 8)

	//ucitavamo podatke
	_, err := reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	hll.M = binary.BigEndian.Uint64(bytes)

	bytes = make([]byte, 1)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	hll.P = uint8(bytes[0])

	hll.Reg = make([]uint8, 0)
	for i:= uint64(0); i < hll.M; i++ {
		bytes = make([]byte, 1)
		_, err := reader.Read(bytes)
		if err != nil {
			log.Fatal(err)
		}
		hll.Reg = append(hll.Reg, bytes...)
	}

	return hll
}

