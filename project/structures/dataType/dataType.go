package dataType

import (
	"fmt"
	"time"
)

type Data struct {
	Value     []byte
	Tombstone bool
	Timestamp uint64
}

func NewData(val []byte, tombstone bool, timestamp uint64) *Data {
	data := new(Data)
	data.Value = val
	data.Tombstone = tombstone
	data.Timestamp = timestamp
	return data
}

func (data *Data) Print() {
	fmt.Println("------------ DATA ------------")
	fmt.Println("Vrednost: " , string(data.Value))
	fmt.Println("Vreme dodavanja: " , time.Unix(int64(data.Timestamp), 0))
}