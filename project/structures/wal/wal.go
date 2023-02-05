package wal

import (
	"encoding/binary"
	"log"
	"os"
	. "project/keyvalue/config"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/entry"
	"strconv"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a Value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type WriteAheadLog struct {
	buffer          []byte
	buffer_capacity uint
	buffer_size     uint
	directory       string
	current_offset  uint
	low_water_mark  uint
}

// inicijalizuje Write Ahead Log i ukoliko logovi vec postoje povecava offset do posle poslednjeg loga
func NewWriteAheadLog(directory string) *WriteAheadLog {
	config := GetConfig()

	//ukoliko ne postoji napravi direktorijum
	_, err := os.Stat(directory)
	if os.IsNotExist(err) {
		err = os.MkdirAll(directory, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}
	wal := new(WriteAheadLog)
	wal.directory = directory
	wal.current_offset = 0
	//ukoliko postoji vec direktorijum sa logovima azuriramo offset
	for {
		filename := wal.generateSegmentFilename()
		_, err := os.Stat(filename)
		if os.IsNotExist(err) {
			break
		}
		wal.current_offset++
	}

	//zadajemo inicijalne vrednosti
	wal.buffer = make([]byte, 0)
	wal.low_water_mark = config.WalWaterMark
	wal.buffer_capacity = config.WalBufferCapacity
	wal.buffer_size = 0
	return wal

}

// generise ime filea sa trenutnim offsetom
func (wal *WriteAheadLog) generateSegmentFilename(offset ...uint) string {
	chosen_offset := wal.current_offset
	if len(offset) > 0 {
		chosen_offset = offset[0]
	}
	filename := wal.directory + "/wal_"
	ustr := strconv.FormatUint(uint64(chosen_offset), 10)

	//upotpunjava ime sa potrebnim nizom nula ukoliko ofset nije vec petocifren broj
	for len(ustr) < 5 {
		ustr = "0" + ustr
	}
	filename += ustr + ".log"
	return filename
}

// kreira file sa narednim offsetom
func (wal *WriteAheadLog) NewWALFile() *os.File {

	filename := wal.generateSegmentFilename()
	wal.current_offset++
	//pravi file u wal direktorijumu
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

// brise sve osim poslednjeg segmenta
func (wal *WriteAheadLog) deleteOldSegments() {
	wal.current_offset--
	for offset := uint(0); offset < wal.current_offset; offset++ {
		err := os.Remove(wal.generateSegmentFilename(offset))
		if err != nil {
			log.Fatal(err)
		}
	}
	//preimenuje poslednji log u prvi i vraca offset na svoje mesto
	err := os.Rename(wal.generateSegmentFilename(wal.current_offset), wal.generateSegmentFilename(0))
	if err != nil {
		log.Fatal(err)
	}
	wal.current_offset = 1
}

// batch zapis - zapisuje ceo buffer u segment sa sledecim offsetom
func (wal *WriteAheadLog) WriteBuffer() {
	//kreira fajl sa narednim offsetom
	file := wal.NewWALFile()

	//zapisujemo ceo buffer u novi fajl
	_, err := file.Write(wal.buffer)
	if err != nil {
		log.Fatal(err)
	}
	wal.buffer = make([]byte, 0)
	wal.buffer_size = 0
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// dodajemo entry u baffer, ukoliko je pun zapisuje buffer u segment
func (wal *WriteAheadLog) addEntryToBuffer(entry *Entry) {
	wal.buffer = append(wal.buffer, EntryToBytes(entry)...)
	wal.buffer_size++
	if wal.buffer_size == wal.buffer_capacity {
		wal.WriteBuffer()
		if wal.current_offset > wal.low_water_mark {
			wal.deleteOldSegments()
		}
	}

}

// zapisuje direktno entry
func (wal *WriteAheadLog) WriteEntry(entry *Entry) {
	//otvaramo file u append only rezimu
	offset := wal.current_offset
	if offset != 0 {
		offset--
	}
	filename := wal.generateSegmentFilename(offset)
	file, err := os.OpenFile(filename, os.O_APPEND, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(filename)
		} else {
			log.Fatal(err)
		}
	}

	//zapisujemo entry kao niz bytova
	_, err = file.Write(EntryToBytes(entry))
	if err != nil {
		log.Fatal(err)
	}
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

	//Proverava da li je prekoracio granicu za brisanje starih
	if wal.current_offset > wal.low_water_mark {
		wal.deleteOldSegments()
	}
}

// cita pojedinacan segment
func (wal *WriteAheadLog) readLog(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		entry.Print()
	}
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

}

// cita hronoloskim redom sve segmente
func (wal *WriteAheadLog) ReadAllLogs() {
	offset := uint(0)
	for offset < wal.current_offset {
		println("==========================================================")
		println("Current offset: ", offset)
		println("==========================================================")
		wal.readLog(wal.generateSegmentFilename(offset))
		offset++
	}
}

// Funkcija ucitava najnoviji segment WAL-a koji ce memtabela koristiti pri kreiranju
// da ne bi bila izgubljena u OM
func (wal *WriteAheadLog) InitiateMemTable() ([]string, []*Data) {
	keys := make([]string, 0)
	dataArr := make([]*Data, 0)

	offset := wal.current_offset
	if offset != 0 {
		offset--
	}

	file, err := os.Open(wal.generateSegmentFilename(offset))
	if err != nil {
		if os.IsNotExist(err) {
			return keys, dataArr
		}
		log.Fatal(err)
	}

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		tombstone := false
		if entry.Tombstone[0] == byte(uint8(1)) {
			tombstone = true
		}
		timestamp := binary.BigEndian.Uint64(entry.Timestamp)
		data := NewData(entry.Value, tombstone, timestamp)
		key := string(entry.Key)

		keys = append(keys, key)
		dataArr = append(dataArr, data)
	}
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
	return keys, dataArr
}
