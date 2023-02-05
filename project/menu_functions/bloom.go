package menu_functions

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	. "project/keyvalue/structures/bloom"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/token_bucket"
	"strconv"
	"strings"
)

func CreateBloomFilter(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *BloomFilter) {
	scanner := bufio.NewScanner(os.Stdin)
    
	var input string //kljuc
	blm := new(BloomFilter)
	var expectedNumOfElem uint32
	var falsePositiveRate float64

	var tempInput string


		input = GetKeyInput()
		if input == "*" {
			return true, input, nil
		}
		input = "BloomFilter" + input
		found, data := GET(input, mem, lru, bucket)
		if found == true {
			var choice string

			for true {

				fmt.Println("Takav kljuc vec postoji u bazi podataka. Da li zelite da:")
				fmt.Println("1. Dobavite ovaj BloomFilter iz baze podataka")
				fmt.Println("2. Napravite novi BloomFilter pod ovim kljucem")
				fmt.Print("Unesite 1 ili 2: ")

				scanner.Scan()
				choice = strings.TrimSpace(scanner.Text())

				err := scanner.Err()
				if choice == "*" {
					return true, input, nil
				}

				if err != nil {
					fmt.Println("Greska prilikom unosa: ", err)
				} else { //ukoliko nema greske:
					if choice == "1" {
						blm = MenuByteToBloomFilter(data.Value)
						return false, input, blm
	
					}else if choice == "2"{
	
						for true {
							fmt.Println("Unesite ocekivani broj elemenata: ")
							scanner.Scan()
							tempInput = strings.TrimSpace(scanner.Text())

							err = scanner.Err()
							if tempInput == "*" {
								return true, input, nil
							}
							if err != nil {
								fmt.Println("Greska prilikom unosa: ", err)
							} else if !IsNumeric(tempInput) {
								fmt.Println("Molimo vas unesite broj.")
							}else {
								tempInt, _ := strconv.ParseUint(tempInput, 10, 64)
								expectedNumOfElem = uint32(tempInt)
								break
							}
					
						}
						for true {
							fmt.Println("Unesite dozvoljeni procenat greske: ")
							scanner.Scan()
							tempInput = strings.TrimSpace(scanner.Text())

							err = scanner.Err()
							if tempInput == "*" {
								return true, input, nil
							}
							if err != nil {
								fmt.Println("Greska prilikom unosa: ", err)
							} else if !IsNumeric(tempInput) {
								fmt.Println("Molimo vas unesite broj.")
							}else {
								tempFloat, _ := strconv.ParseFloat(tempInput, 64)
								falsePositiveRate = tempFloat
								break
							}
					
						}
	
						blm = NewBloomFilter(expectedNumOfElem, falsePositiveRate)
						return false, input, blm
					} else{
						fmt.Println("Molimo vas unesite 1 ili 2")
					}
				}

				
			}
			
			return true, input, nil
		}else{

			for true {
				fmt.Print("Unesite ocekivani broj elemenata: ")
				scanner.Scan()
				tempInput = strings.TrimSpace(scanner.Text())

				err := scanner.Err()
				if tempInput == "*" {
					return true, input, nil
				}
				if err != nil {
					fmt.Println("Greska prilikom unosa: ", err)
				} else if !IsNumeric(tempInput) {
					fmt.Println("Molimo vas unesite broj.")
				}else {
					tempInt, _ := strconv.ParseUint(tempInput, 10, 64)
					expectedNumOfElem = uint32(tempInt)
					break
				}
		
			}
			for true {
				fmt.Print("Unesite dozvoljeni procenat greske: ")
				scanner.Scan()
				tempInput = strings.TrimSpace(scanner.Text())

				err := scanner.Err()
				if tempInput == "*" {
					return true, input, nil
				}
				if err != nil {
					fmt.Println("Greska prilikom unosa: ", err)
				} else if !IsNumeric(tempInput) {
					fmt.Println("Molimo vas unesite broj.")
				}else {
					tempFloat, _ := strconv.ParseFloat(tempInput, 64)
					falsePositiveRate = tempFloat
					break
				}
		
			}
			blm = NewBloomFilter(expectedNumOfElem, falsePositiveRate)

	}
	fmt.Println()
	return false, input, blm
}

func GetBloomFilter(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *BloomFilter) {
	var key string
	blm := new(BloomFilter)

	//unos
	key = GetKeyInput()
	if key == "*" {
		return false, key, nil
	}
	key = "BloomFilter" + key
	
	found, data := GET(key, mem, lru, bucket)
	if found {
		cmsBytes := data.Value
		blm = MenuByteToBloomFilter(cmsBytes)
		fmt.Println("Uspesno dobavljanje")
		return true, key, blm
	}
	return false, key, nil
}

func BloomFilterAddElement(blm *BloomFilter) {
	var val []byte

	//unos
	val = GetValueInput()

	if bytes.Compare(val, []byte("*")) == 0 { 	//ukoliko je uneta *
		return
	}
	blm.AddToBloom(val)
	fmt.Println("Uspesno dodavanje")
}

//vraca da li se podatak mozda nalazi u bloom filteru
func BloomFilterFindElem(blm *BloomFilter) {
	var val []byte

	//unos
	val = GetValueInput()

	if bytes.Compare(val, []byte("*")) == 0 { 	//ukoliko je uneta *
		return
	}

	found := blm.IsInBloom(val)

	if found {
		fmt.Println("Podatak se mozda nalazi u BloomFilteru")
	}
	
	if !found {
		fmt.Println("Podatak se ne nalazi u BloomFilteru")
	}
}

func BloomFilterPUT(key string, blm *BloomFilter, mem MemTable, bucket *TokenBucket) {
	bytesBLM := MenuBloomFilterToByte(blm)
	PUT(key, bytesBLM, mem, bucket)
}

func BloomFilterMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	scanner := bufio.NewScanner(os.Stdin)
	activeBLM := new(BloomFilter)
	var activeKey string //kljuc Bloom filtera
	var userkey string   //kljuc koji je korisnik uneo i koji se ispisuje korisniku
	userkey = ""
	for true {

		fmt.Println("=======================================")
		fmt.Print("Kljuc aktivnog Bloom filtera: ")
		fmt.Println(userkey)
		fmt.Println("Broj elemenata: ", activeBLM.N)
		fmt.Println("Velicina bitseta: ", activeBLM.M)
		fmt.Println("Broj hash funkcija: ", activeBLM.K)
		fmt.Println()
		fmt.Println("1 - Kreiraj bloom filter")
		fmt.Println("2 - Dobavi bloom filter iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Pronadji element")
		fmt.Println("5 - Upisi bloom filter u bazu podataka")
		fmt.Println("6 - Obrisi bloom filter iz baze podataka")
		fmt.Println("X - Povratak na glavni meni")
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
			found, tempKey, tempBLM := CreateBloomFilter(mem, lru, bucket)
				if !found {
					activeBLM = tempBLM
					activeKey = tempKey
					userkey = activeKey[11:]
				}

		case "2":
			found, tempKey, tempBLM := GetBloomFilter(mem, lru, bucket)
			if tempKey != "*" {
				if found {
					activeBLM = tempBLM
					activeKey = tempKey
					userkey = activeKey[11:]
				} else {
					fmt.Println("Ne postoji BloomFilter sa datim kljucem")
				}
			}
		case "3":

			if len(activeKey) != 0 {
				BloomFilterAddElement(activeBLM)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}

		case "4":
			if len(activeKey) != 0 {
				BloomFilterFindElem(activeBLM)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}
		case "5":
			if len(activeKey) != 0 {
				BloomFilterPUT(activeKey, activeBLM, mem, bucket)
				fmt.Println("Uspesan upis")
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}
		case "6":
			if len(activeKey) != 0 {
				DELETE(activeKey, mem, lru, bucket)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
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