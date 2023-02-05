package menu_functions

import (
	"bufio"
	"fmt"
	"os"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/simhash"
	. "project/keyvalue/structures/token_bucket"
	"strings"
)

//Generise i upisuje u bazu podataka binarni kod
func SimHashGenerateBinaryHash(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	var value []byte

	key := GetKeyInput()
	if key != "*" {
		key = "SimHash" + key
		value = GetValueInput()
	
		binaryHash := HashText(GenerateWeightedMap(value))
		binaryBytes := BinaryHashToByte(binaryHash)
		PUT(key, binaryBytes, mem, bucket)
		fmt.Println("Uspesan upis.")
	}

}

//Ukoliko se kljucevi nalaze u datoteci poredi ih i vraca hemingovo rastojanje izmedju vrednosti
func SimHashCompare(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	var found1 bool
	data1 := new(Data)
	var found2 bool
	data2 := new(Data)
	fmt.Println("Unesite prvi pa zatim drugi kljuc")
	fmt.Println("Kljuc 1 : ")
	for true {

		key1 := GetKeyInput()
		if key1 == "*" {
			return
		}
		key1 = "SimHash" + key1

		found1, data1 = GET(key1, mem, lru, bucket)

		if found1 == false {
			fmt.Println("Kljuc 1 se ne nalazi u bazi podataka.")
		} else {
			break
		}
	}
	fmt.Println("Kljuc 2 : ")
	for true {

		key2 := GetKeyInput()
		if key2 == "*" {
			return
		}
		key2 = "SimHash" + key2

		found2, data2 = GET(key2, mem, lru, bucket)

		if found2 == false {
			fmt.Println("Kljuc 2 se ne nalazi u bazi podataka.")
		} else {
			break
		}
	}
	a := ByteToBinaryHash(data1.Value)
	b := ByteToBinaryHash(data2.Value)

	fmt.Print("Slicnost ova dva podatka iznosi: ")
	fmt.Println(Compare(a, b))
}


func SimHashMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	scanner := bufio.NewScanner(os.Stdin)
	for true {

		fmt.Println("=======================================")
		fmt.Println("1 - Generisi binarni hash kod i upisi ga")
		fmt.Println("2 - Uporedi dva SimHash podatka")
		fmt.Println("3 - Obrisi SimHash podatak")
		fmt.Println("X - povratak na glavni meni")
		fmt.Println("=======================================")
		fmt.Print("Izaberite opciju: ")

		var input string
		scanner.Scan()
		input = strings.TrimSpace(scanner.Text())

		err := scanner.Err()
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		}
		
		switch input {
		case "1":
			SimHashGenerateBinaryHash(mem, lru, bucket)


		case "2":
			SimHashCompare(mem, lru, bucket)
		case "3":
			key := GetKeyInput()
			if key != "*" {
				key = "SimHash" + key
				DELETE(key, mem, lru, bucket)
				fmt.Println("Uspesno brisanje")
			}

		case "x":
			return
		case "X":
			return
		default:
			fmt.Println("Neispravan unos. Molimo vas probajte opet.")
		}
	}
}