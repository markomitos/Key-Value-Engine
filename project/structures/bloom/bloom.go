package bloom

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
)

type BloomFilter struct {
	Bitset    []bool // niz
	K         uint32 // Number of hash values
	N         uint32 // Number of elements in the filter
	M         uint32 // Size of the bloom filter bitset
	HashFuncs []HashWithSeed
}

// Konstruktor
func NewBloomFilter(expectedNumOfElem uint32, falsePositiveRate float64) *BloomFilter {
	blm := new(BloomFilter)
	blm.M = CalculateM(expectedNumOfElem, falsePositiveRate)
	blm.K = CalculateK(expectedNumOfElem, blm.M)
	blm.N = 0
	blm.HashFuncs = CreateHashFunctions(blm.K)
	blm.Bitset = make([]bool, blm.M)
	return blm
}

//Dodaje element u bloom filter
func (blm *BloomFilter) AddToBloom(elem []byte) {
	blm.N++
	for _, fn := range blm.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(blm.M)))
		blm.Bitset[hashedValue] = true
	}
}

//Proverava da li se kljuc mozda nalazi ili sigurno ne nalazi u bloom filteru
func (blm *BloomFilter) IsInBloom(elem []byte) bool {
	for _, fn := range blm.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(blm.M)))
		if blm.Bitset[hashedValue] == false {
			return false
		}
	}
	return true
}

//Pretvara niz boolova u bajtove
func MenuBoolsToBytes(t []bool) []byte {
	b := make([]byte, (len(t)+7)/8)
	for i, x := range t {
		if x {
			b[i/8] |= 0x80 >> uint(i%8)
		}
	}
	return b
}

//Pretvara niz bajtova u boolove
func MenuBytesToBools(b []byte) []bool {
	t := make([]bool, 8*len(b))
	for i, x := range b {
		for j := 0; j < 8; j++ {
			if (x<<uint(j))&0x80 == 0x80 {
				t[8*i+j] = true
			}
		}
	}
	return t
}

//Pretvara bloomfilter u bajtove
func MenuBloomFilterToByte(blm *BloomFilter) []byte {
	//Zapisujemo konstante
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(blm.K))

	bytesN := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesN, uint32(blm.N))
	bytes = append(bytes, bytesN...)

	bytesM := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesM, uint32(blm.M))
	bytes = append(bytes, bytesM...)

	//pretvaramo niz bool u bytes
	bitsetByte := MenuBoolsToBytes(blm.Bitset)

	//belezimo duzinu bitseta
	bytesBitSetLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesBitSetLen, uint32(len(bitsetByte)))
	bytes = append(bytes, bytesBitSetLen...)

	bytes = append(bytes, bitsetByte...)
	for _, fn := range blm.HashFuncs {
		//Belezimo duzinu svake hashfunkcije
		bytesHFLen := make([]byte, 4)
		binary.BigEndian.PutUint32(bytesHFLen, uint32(len(fn.Seed)))
		bytes = append(bytes, bytesHFLen...)

		//zapisuje hashfunkciju
		bytes = append(bytes, fn.Seed...)
	}

	return bytes
}

//Pretvara niz bajtova u bloomfilter
func MenuByteToBloomFilter(blmBytes []byte) *BloomFilter {
	blm := new(BloomFilter)
	reader := bytes.NewReader(blmBytes)
	bytes := make([]byte, 4)

	//Ucitavamo konstante
	_, err := reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.K = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.N = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.M = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	bitsetSize := binary.BigEndian.Uint32(bytes)

	//Ucitavamo bitset
	bytes = make([]byte, bitsetSize)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.Bitset = MenuBytesToBools(bytes)
	blm.Bitset = blm.Bitset[0:blm.M] //Osisamo visak u poslednjem bajtu

	blm.HashFuncs = make([]HashWithSeed, 0)
	hashWithSeed := new(HashWithSeed)
	//Ucitavamo svaku hashfunkciju
	for i := uint32(0); i < blm.K; i++ {
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
		blm.HashFuncs = append(blm.HashFuncs, *hashWithSeed)
	}

	return blm
}