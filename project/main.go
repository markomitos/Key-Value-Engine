package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	// "strconv"

	"time"

	// "time"

	// . "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/lsm"
	. "project/keyvalue/structures/memtable"

	. "project/keyvalue/menu_functions"
	. "project/keyvalue/structures/token_bucket"
	. "project/keyvalue/structures/wal"
	// . "project/keyvalue/structures/sstable"
)

func menu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	fmt.Println("=======================================")
	fmt.Println("============= GLAVNI MENI =============")
	fmt.Println("=======================================")
	fmt.Println("Ukoliko zelite da izadjete iz bilo koje funkcije, UNESITE '*'")
	fmt.Println("1 - PUT")
	fmt.Println("2 - GET")
	fmt.Println("3 - DELETE")
	fmt.Println("4 - LIST")
	fmt.Println("5 - RANGE SCAN")
	fmt.Println("6 - KOMPAKCIJA")
	fmt.Println("7 - CountMinSketch menu")
	fmt.Println("8 - BloomFilter menu")
	fmt.Println("9 - HyperLogLog menu")
	fmt.Println("10 - SimHash menu")
	fmt.Println("11 - Ispisi sve podatke")
	fmt.Println("12 - Generisanje unosa")
	fmt.Println("X - Izlaz iz programa")
	fmt.Println("=======================================")
	fmt.Print("Izaberite opciju: ")

	var input string


	scanner := bufio.NewScanner(os.Stdin)
    scanner.Scan()
	input = strings.TrimSpace(scanner.Text())

	err := scanner.Err()
	if err != nil {
		fmt.Println("Greska prilikom unosa!")
	}
	switch input {
	case "1":
		key, val := GetUserInput()
		if key != "*"{
			PUT(key, val, mem, bucket)
			fmt.Println("Uspesno dodavanje")
		}
	case "2":
		key:= GetKeyInput()
		if key != "*"{
			start := time.Now()
			found, data := GET(key, mem, lru, bucket)
			elapsedTime := time.Since(start)
			if found {
				data.Print()
				fmt.Printf("Vreme dobavljanja podatka: %s\n", elapsedTime)
			} else {
				fmt.Println("Kljuc se ne nalazi u bazi podataka")
			}
		}
		
	case "3":
		key:= GetKeyInput()
		if key != "*"{
			DELETE(key, mem, lru, bucket)
			fmt.Println("Uspesno brisanje")
		}
	case "4":
		InitiateListScan(mem, bucket)
	case "5":
		InitiateRangeScan(mem, bucket)
	case "6":
		RunCompact()
	case "7":
		CountMinSKetchMenu(mem, lru, bucket)
	case "8":
		BloomFilterMenu(mem, lru, bucket)
	case "9":
		HyperLogLogMenu(mem, lru, bucket)
	case "10":
		SimHashMenu(mem, lru, bucket)
	case "11":
		mem.Print()
		ReadLsm().Print()
	case "12":
		GenerateEntries(mem, bucket)
	case "x":
		fmt.Println("Uspesan izlaz!")
		os.Exit(0)
	case "X":
		fmt.Println("Uspesan izlaz!")
		os.Exit(0)
	default:
		fmt.Println("Neispravan unos. Molimo vas probajte opet.")
	}
}

func main() {
	//inicijalizujemo strukturu fajlova
	InitializeLsm()
	
	//Ucitavamo CACHE (LRU)
	lru := ReadLru()

	//Na pocetku ucitavamo iz WAL-a u memtabelu
	wal := NewWriteAheadLog("files/wal")
	memtable := LoadToMemTable(wal.InitiateMemTable())

	//Ogranicenje brzine pristupa
	bucket := NewTokenBucket()

	for true {
		menu(memtable, lru, bucket)
	}
	
}

