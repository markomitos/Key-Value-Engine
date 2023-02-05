package entry

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	. "project/keyvalue/structures/dataType"
)

// struktura za svaki pojedinacni zapis
type Entry struct {
	Crc        []byte
	Timestamp  []byte
	Tombstone  []byte
	Key_size   []byte
	Value_size []byte
	Key        []byte
	Value      []byte
}

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE //4
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE //12
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE //13
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE //21
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE //29
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// Konstruktor jednog unosa
func NewEntry(key string, data *Data) *Entry {
	e := new(Entry)

	keyBytes := []byte(key)

	//izracunaj duzinu kljuca i vrednosti
	e.Key_size = make([]byte, 8)
	e.Value_size = make([]byte, 8)
	binary.BigEndian.PutUint64(e.Key_size, uint64(int64(len(keyBytes))))
	binary.BigEndian.PutUint64(e.Value_size, uint64(int64(len(data.Value))))

	e.Key = keyBytes
	e.Value = data.Value
	e.Timestamp = make([]byte, 8)
	binary.BigEndian.PutUint64(e.Timestamp, data.Timestamp)

	tombstoneBytes := make([]byte, 0)
	if data.Tombstone {
		tombstoneBytes = append(tombstoneBytes, uint8(1))
	} else {
		tombstoneBytes = append(tombstoneBytes, uint8(0))
	}
	e.Tombstone = tombstoneBytes

	//ubaci sve u niz bajtova da bi napravio Crc
	bytes := make([]byte, 0)
	bytes = append(bytes, e.Timestamp...)
	bytes = append(bytes, e.Tombstone...)
	bytes = append(bytes, e.Key_size...)
	bytes = append(bytes, e.Value_size...)
	bytes = append(bytes, e.Key...)
	bytes = append(bytes, e.Value...)
	e.Crc = make([]byte, 4)
	binary.BigEndian.PutUint32(e.Crc, uint32(CRC32(bytes)))
	return e
}

// pretvara iz Entry u niz bitova da bi mogli da zapisemo u fajlu
func EntryToBytes(e *Entry) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, e.Crc...)
	bytes = append(bytes, e.Timestamp...)
	bytes = append(bytes, e.Tombstone...)
	bytes = append(bytes, e.Key_size...)
	bytes = append(bytes, e.Value_size...)
	bytes = append(bytes, e.Key...)
	bytes = append(bytes, e.Value...)
	return bytes
}

// pretvara iz niza bytova u Entry da bi mogli da procitamo vrednosti iz fajla
func BytesToEntry(bytes []byte) *Entry {
	e := new(Entry)
	e.Crc = bytes[CRC_START:TIMESTAMP_START]
	e.Timestamp = bytes[TIMESTAMP_START:TOMBSTONE_START]
	e.Tombstone = bytes[TOMBSTONE_START:KEY_SIZE_START]
	e.Key_size = bytes[KEY_SIZE_START:VALUE_SIZE_START]
	e.Value_size = bytes[VALUE_SIZE_START:KEY_START]
	e.Key = bytes[KEY_START : KEY_START+binary.BigEndian.Uint64(e.Key_size)]
	e.Value = bytes[KEY_START+binary.BigEndian.Uint64(e.Key_size) : KEY_START+binary.BigEndian.Uint64(e.Key_size)+binary.BigEndian.Uint64(e.Value_size)]
	return e
}

// cita niz bitova i pretvara ih u klasu entity za dalju obradu
func ReadEntry(file *os.File) *Entry {
	//prvo procitamo do kljuca da bi videli koje su  velicine kljuc i vrednost
	bytes := make([]byte, KEY_START)
	_, err := file.Read(bytes)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Fatal(err)
	}
	//procitamo velicine kljuca i vrednosti
	Key_size := bytes[KEY_SIZE_START:VALUE_SIZE_START]
	Value_size := bytes[VALUE_SIZE_START:]
	//procitamo kljuc
	Key := make([]byte, int(binary.BigEndian.Uint64(Key_size)))
	_, err = file.Read(Key)
	if err != nil {
		log.Fatal(err)
	}

	//procitamo vrednost
	Value := make([]byte, int(binary.BigEndian.Uint64(Value_size)))
	_, err = file.Read(Value)
	if err != nil {
		log.Fatal(err)
	}

	bytes = append(bytes, Key...)
	bytes = append(bytes, Value...)

	entry := BytesToEntry(bytes)

	return entry
}

// ispis pojedinacnog unosa
func (entry *Entry) Print() {
	Timestamp := binary.BigEndian.Uint64(entry.Timestamp)
	Key_size := binary.BigEndian.Uint64(entry.Key_size)
	Value_size := binary.BigEndian.Uint64(entry.Value_size)
	//Tombstone
	tombstone := false
	if entry.Tombstone[0] == byte(uint8(1)) {
		tombstone = true
	}
	println("Entry: ")
	println("CRC: ", entry.Crc)
	println("Timestamp: ", Timestamp)
	println("Tombstone: ", tombstone)
	println("Key size: ", Key_size)
	println("Value size: ", Value_size)
	println("Key: ", string(entry.Key))
	println("Value: ", entry.Value)
	println("---------------------------------------")
}