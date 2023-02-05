package memtable

import (
	"log"
	. "project/keyvalue/config"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/lsm"
	. "project/keyvalue/structures/scan"
	. "project/keyvalue/structures/skiplist"
	. "project/keyvalue/structures/sstable"
	. "project/keyvalue/structures/wal"
	"time"
)

type MemTableList struct {
	size  uint
	slist *SkipList
}

// konstuktor za skiplistu
func NewMemTableList(s uint) *MemTableList {
	config := GetConfig()
	m := new(MemTableList)
	m.slist = NewSkipList(config.SkiplistMaxHeight)
	m.size = s
	return m
}

func (m *MemTableList) Print() {
	m.slist.Print()
}

//Trazi zadati kljuc u memtabeli
func (m *MemTableList) Find(key string) (bool, *Data) {
	node, found := m.slist.Find(key)
	if !found {
		return false, nil
	}
	return true, node.Data
}

//Flush na disk -> kreira novu sstabelu
func (m *MemTableList) Flush() {
	keys := make([]string, 0)
	values := make([]*Data, 0)
	//dobavi sve sortirane podatke
	m.slist.GetAllNodes(&keys, &values)

	//praznjenje skipliste
	newSkiplist := NewSkipList(m.size)
	m.slist = newSkiplist

	//Flush
	sstable := NewSSTable(uint32(m.size), GenerateFlushName())
	sstable.Flush(keys, values)
	IncreaseLsmLevel(1)

	//WAL -> kreiramo novi segment(log)
	err := NewWriteAheadLog("files/wal").NewWALFile().Close()
	if err != nil {
		log.Fatal(err)
	}
}

//Ubacuje element u memtabelu
func (m *MemTableList) Put(key string, data *Data) {
	m.slist.Put(key, data)

	if m.slist.GetSize() == m.size {
		m.Flush()
	}
}

//Brise element iz memtabele
func (m *MemTableList) Remove(key string) {
	//Ukoliko nije nasao trazeni kljuc u Memtable
	//Dodaje ga kao novi element sa tombstone=true
	if !m.slist.Remove(key) {
		data := new(Data)
		data.Timestamp = uint64(time.Now().Unix())
		data.Tombstone = true
		data.Value = make([]byte, 0)
		m.Put(key, data)
	}
}

// Trazi podatke ciji kljucevi spadaju u dati opseg
func (m *MemTableList) RangeScan(minKey string, maxKey string, scan *Scan) {
	m.slist.RangeScan(minKey, maxKey, scan)
}

// Trazi podatke ciji kljucevi imaju dati prefix
func (m *MemTableList) ListScan(prefix string, scan *Scan) {
	m.slist.ListScan(prefix, scan)
}
