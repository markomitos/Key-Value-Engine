package memtable

import (
	"log"
	. "project/keyvalue/config"
	. "project/keyvalue/structures/b_tree"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/lsm"
	. "project/keyvalue/structures/scan"
	. "project/keyvalue/structures/sstable"
	. "project/keyvalue/structures/wal"
	"time"
)

type MemTableTree struct {
	size  uint
	btree *BTree
}

// konstruktor za b stablo
func NewMemTableTree(s uint) *MemTableTree {
	config := GetConfig()
	m := new(MemTableTree)
	m.size = s
	m.btree = NewBTree(config.BTreeNumOfChildren)
	return m

}

func (m *MemTableTree) Print() {
	m.btree.PrintBTree()
}

//Trazi zadati kljuc u memtabeli
func (m *MemTableTree) Find(key string) (bool, *Data) {
	found, node := m.btree.FindNode(key)
	if !found {
		return false, nil
	}
	return true, node.Values[key]
}

//Flush na disk -> kreira novu sstabelu
func (m *MemTableTree) Flush() {
	config := GetConfig()

	//dobavi sve sortirane podatke
	keys := make([]string, 0)
	values := make([]*Data, 0)
	m.btree.InorderTraverse(m.btree.Root, &keys, &values)

	//praznjenje b_stabla i rotacija
	newBTree := NewBTree(config.BTreeNumOfChildren)
	m.btree = newBTree

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
func (m *MemTableTree) Put(key string, data *Data) {
	m.btree.Put(key, data)

	if m.btree.Size == m.size {
		m.Flush()
	}
}

//Brise element iz memtabele
func (m *MemTableTree) Remove(key string) {
	//Ukoliko nije nasao trazeni kljuc u Memtable
	//Dodaje ga kao novi element sa tombstone=true
	if !m.btree.Remove(key) {
		data := new(Data)
		data.Timestamp = uint64(time.Now().Unix())
		data.Tombstone = true
		data.Value = make([]byte, 0)
		m.Put(key, data)
	}
}

// Trazi podatke ciji kljucevi spadaju u dati opseg
func (m *MemTableTree) RangeScan(minKey string, maxKey string, scan *Scan) {
	m.btree.RangeScan(minKey, maxKey, m.btree.Root, scan)
}

// Trazi podatke ciji kljucevi imaju dati prefix
func (m *MemTableTree) ListScan(prefix string, scan *Scan) {
	m.btree.ListScan(prefix, m.btree.Root, scan)
}
