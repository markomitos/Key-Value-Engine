package cms

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
)

type CountMinSketch struct {
	K          uint32    //broj hashFunkcija (dubina)
	M          uint32    //broj kolona (sirina)
	Epsilon    float64 //Preciznost
	Delta      float64 //Sigutnost tacnosti (falsePositive rate)
	ValueTable [][]uint32
	HashFuncs  []CmsHashWithSeed
}

//Konstruktor
func NewCountMinSketch(Epsilon float64, Delta float64) *CountMinSketch {
	cms := new(CountMinSketch)
	cms.K = CmsCalculateK(Delta)
	cms.M = CmsCalculateM(Epsilon)
	cms.Epsilon = Epsilon
	cms.Delta = Delta
	cms.HashFuncs = CmsCreateHashFunctions(cms.K)
	cms.ValueTable = make([][]uint32, cms.K)
	for i := range cms.ValueTable {
		cms.ValueTable[i] = make([]uint32, cms.M)
	}
	return cms
}

//Dodaje element u cms
func AddToCms(cms *CountMinSketch, elem []byte) {
	for i, fn := range cms.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.M)))
		cms.ValueTable[i][hashedValue]++
	}
}

//Vraca procenjenu kardinalnost elementa
func CheckFrequencyInCms(cms *CountMinSketch, elem []byte) uint32 {
	//Pomocni slice pomocu kojeg racuna min (sastoji se od svih vrednosti)
	arr := make([]uint32, cms.K)
	for i, fn := range cms.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.M)))
		arr[i] = cms.ValueTable[i][hashedValue]
	}
	min := arr[0]
	for _, v := range arr {
		if v < min {
			min = v
		}
	}
	return min
}

//Pretvara cms u bajtove
func CountMinSkechToBytes(cms *CountMinSketch) []byte {

	//upisujemo promenljive tipa uint32
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(cms.K))

	bytesM := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesM, uint32(cms.M))
	bytes = append(bytes, bytesM...)

	//upisujemo promenljive tipa float64
	bitsEpsilon := math.Float64bits(cms.Epsilon)
	bytesEpsilon := make([]byte, 8)
	binary.BigEndian.PutUint64(bytesEpsilon, bitsEpsilon)
	bytes = append(bytes, bytesEpsilon...)

	bitsDelta := math.Float64bits(cms.Epsilon)
	bytesDelta := make([]byte, 8)
	binary.BigEndian.PutUint64(bytesDelta, bitsDelta)
	bytes = append(bytes, bytesDelta...)

	//upisujemo podatak po podatak iz valuetable-a
	for i:= 0; i < int(cms.K); i++ {
		for j:= 0; j < int(cms.M); j++ {
			bytesElem := make([]byte, 4)
			binary.BigEndian.PutUint32(bytesElem, uint32(cms.ValueTable[i][j]))
			bytes = append(bytes, bytesElem...)
		}
	}

	for _, fn := range cms.HashFuncs {
		//Belezimo duzinu svake hashfunkcije
		bytesHFLen := make([]byte, 4)
		binary.BigEndian.PutUint32(bytesHFLen, uint32(len(fn.Seed)))
		bytes = append(bytes, bytesHFLen...)

		//zapisuje hashfunkciju
		bytes = append(bytes, fn.Seed...)
	}
	return bytes
}

//Pretvara bajtove u cms
func BytesToCountMinSketch(CmsBytes []byte) *CountMinSketch {
	cms := new(CountMinSketch)
	reader := bytes.NewReader(CmsBytes)
	bytes := make([]byte, 4)

	//Ucitavamo podatke tipa uint32
	_, err := reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	cms.K = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	cms.M = binary.BigEndian.Uint32(bytes)

	//ucitavamo podatke tipa float64
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	bitsEpsilon := binary.BigEndian.Uint64(bytes)
	floatEpsilon := math.Float64frombits(bitsEpsilon)
	cms.Epsilon = floatEpsilon

	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	bitsDelta := binary.BigEndian.Uint64(bytes)
	floatDelta := math.Float64frombits(bitsDelta)
	cms.Delta = floatDelta

	//kreiramo ValueTable
	cms.ValueTable = make([][]uint32, cms.K)
	for i := range cms.ValueTable {
		cms.ValueTable[i] = make([]uint32, cms.M)
	}

	for i:= 0; i < int(cms.K); i++ {
		for j:= 0; j < int(cms.M); j++ {
			bytes := make([]byte, 4)
			_, err = reader.Read(bytes)
			if err != nil {
				log.Fatal(err)
			}
			cms.ValueTable[i][j] = binary.BigEndian.Uint32(bytes)
		}
	}

	cms.HashFuncs = make([]CmsHashWithSeed, 0)
	hashWithSeed := new(CmsHashWithSeed)
	//Ucitavamo svaku hashfunkciju
	for i := uint32(0); i < cms.K; i++ {
		//Ucitavamo duzinu trenutne hf
		bytes = make([]byte, 4)
		_, err = reader.Read(bytes)
		if err != nil {
			log.Fatal(err)
		}
		hashFuncLen := binary.BigEndian.Uint32(bytes)

		//citamo hf
		bytes = make([]byte, hashFuncLen)
		_, err = reader.Read(bytes)
		if err != nil {
			log.Fatal(err)
		}
		hashWithSeed.Seed = bytes
		cms.HashFuncs = append(cms.HashFuncs, *hashWithSeed)
	}
	return cms
}
