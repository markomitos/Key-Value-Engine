package memtable

import (
	. "project/keyvalue/config"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/scan"
)

// da bi mogli nad oba tipa napisati funkcije pravimo interface
type MemTable interface {
	Put(key string, data *Data)
	Find(key string) (bool, *Data)
	Remove(key string)
	Flush()
	Print()
	RangeScan(minKey string, maxKey string, scan *Scan)
	ListScan(prefix string, scan *Scan)
}

//Konstruktor za memtabelu
func NewMemTable(s uint) MemTable{
	config := GetConfig()
	var memTable MemTable
	if config.MemtableStructure == "b_tree"{
		memTable = NewMemTableTree(s)
	} else if config.MemtableStructure == "skiplist"{
		memTable = NewMemTableList(s)
	}
	return memTable
}

//Poziva se pri ucitavanju iz wal-a
//Smesta niz kljuceva i vrednosti u memoriju
func LoadToMemTable(keys []string, data []*Data) MemTable{
	config := GetConfig()
	memtable := NewMemTable(config.MemtableSize)
	for i:=0; i < len(keys); i++{
		memtable.Put(keys[i], data[i])
	}
	return memtable
}

