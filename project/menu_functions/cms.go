package menu_functions

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	. "project/keyvalue/structures/cms"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/token_bucket"
	"strconv"
	"strings"
)

func CreateCountMinSketch(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *CountMinSketch) {
	scanner := bufio.NewScanner(os.Stdin)
	var input string
	var epsilon float64
	var delta float64

	var tempInput string
	cms := new(CountMinSketch)
	for true{

		input = GetKeyInput()
		if input == "*" {
			return true, input, nil
		}
		input = "CountMinSketch" + input
		found, data := GET(input, mem, lru, bucket)
		if found == true {
			var choice string

			for true {

				fmt.Println("Takav kljuc vec postoji u bazi podataka. Da li zelite da:")
				fmt.Println("1. Dobavite ovaj CountMinSketch iz baze podataka")
				fmt.Println("2. Napravite novi CountMinSketch pod ovim kljucem")
				fmt.Print("Unesite 1 ili 2: ")
				scanner.Scan()
				choice = strings.TrimSpace(scanner.Text())
				err := scanner.Err()
				if err != nil {
					fmt.Println("Greska tokom unosa!")
				}

				if choice == "*" {
					return true, input, nil
				}

				if choice == "1" {
					cms = BytesToCountMinSketch(data.Value)
					return false, input, cms

				}else if choice == "2"{

					for true {
						fmt.Print("Unesite preciznost (epsilon): ")
						scanner.Scan()
						tempInput = strings.TrimSpace(scanner.Text())

						err := scanner.Err()
						if tempInput == "*" {
							return true, tempInput, nil
						}
						if err != nil {
							fmt.Println("Greska prilikom unosa: ", err)
						}else if !IsNumeric(tempInput) {
							fmt.Println("Molimo vas unesite broj.")
						}else {
							tempInt, _ := strconv.ParseFloat(tempInput, 64)
							epsilon = tempInt
							break
						}
					}
					for true {
						fmt.Print("Unesite sigurnost tacnosti (delta): ")
						scanner.Scan()
						tempInput = strings.TrimSpace(scanner.Text())

						err := scanner.Err()
						if tempInput == "*" {
							return true, tempInput, nil
						}
						if err != nil {
							fmt.Println("Greska prilikom unosa: ", err)
						}else if !IsNumeric(tempInput) {
							fmt.Println("Molimo vas unesite broj.")
						}else {
							tempInt, _ := strconv.ParseFloat(tempInput, 64)
							delta = tempInt
							break
						}
					}

					cms = NewCountMinSketch(epsilon, delta)
					return false, input, cms
				} else{
					fmt.Println("Molimo vas unesite 1 ili 2")
				}
			}
			return true, input, nil
		}else {

			for true {
				fmt.Print("Unesite preciznost (epsilon): ")
				scanner.Scan()
				tempInput = strings.TrimSpace(scanner.Text())

				err := scanner.Err()
				if tempInput == "*" {
					return true, tempInput, nil
				}
				if err != nil {
					fmt.Println("Greska prilikom unosa: ", err)
				}else if !IsNumeric(tempInput) {
					fmt.Println("Molimo vas unesite broj.")
				}else {
					tempInt, _ := strconv.ParseFloat(tempInput, 64)
					epsilon = tempInt
					break
				}
			}
			for true {
				fmt.Print("Unesite sigurnost tacnosti (delta): ")
				scanner.Scan()
				tempInput = strings.TrimSpace(scanner.Text())

				err := scanner.Err()
				if tempInput == "*" {
					return true, tempInput, nil
				}
				if err != nil {
					fmt.Println("Greska prilikom unosa: ", err)
				}else if !IsNumeric(tempInput) {
					fmt.Println("Molimo vas unesite broj.")
				}else {
					tempInt, _ := strconv.ParseFloat(tempInput, 64)
					delta = tempInt
					break
				}
			}
			cms = NewCountMinSketch(epsilon, delta)

			break
		}
	}

	return false, input, cms
}
//dobavlja cms iz baze podataka
func CountMinSketchGET(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *CountMinSketch) {
	var key string
	cms := new(CountMinSketch)

	//unos
	fmt.Print("Unesite kljuc: ")
	key = GetKeyInput()
	if key == "*" {
		return false, key, nil
	}
	key = "CountMinSketch" + key
	
	found, data := GET(key, mem, lru, bucket)
	if found {
		cmsBytes := data.Value
		cms = BytesToCountMinSketch(cmsBytes)
		return true, key, cms
	}
	return false, key, nil

}

func CountMinSketchAddElement(cms *CountMinSketch) {
	var val []byte

	//unos
	fmt.Println("Unesite podatak koji zelite da dodate: ")
	val = GetValueInput()
	if bytes.Compare(val, []byte("*")) == 0 { //ukoliko je zvezdica, izadji iz funkcije
		return
	}
	AddToCms(cms, val)
	fmt.Println("Uspesno dodavanje")
}

func CountMinSketchCheckFrequency(cms *CountMinSketch) {
	var val []byte

	//unos
	fmt.Println("Unesite podatak koji zelite da proverite: ")
	val = GetValueInput()
	if bytes.Compare(val, []byte("*")) == 0 { //ukoliko je zvezdica, izadji iz funkcije
		return
	}

	freq := CheckFrequencyInCms(cms, val)

	fmt.Print("Broj ponavljanja podatka iznosi: ")
	fmt.Println(freq)
}

func CountMinSketchPUT(key string, cms *CountMinSketch, mem MemTable, bucket *TokenBucket) {
	bytesCms := CountMinSkechToBytes(cms)
	PUT(key, bytesCms, mem, bucket)
	fmt.Println("Uspesno dodavanje")
}

func CountMinSketchDELETE(key string, mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	DELETE(key, mem, lru, bucket)
}



func CountMinSKetchMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	scanner := bufio.NewScanner(os.Stdin)
	activeCMS := new(CountMinSketch)
	var activeKey string //kljuc CMS-a
	var userkey string //kljuc koji je korisnik uneo i koji se ispisuje korisniku
	userkey = ""
	for true {

		fmt.Println("=======================================")
		fmt.Print("Kljuc aktivnog CountMinSketch-a: ")
		fmt.Println(userkey)
		fmt.Println("Preciznost: ", activeCMS.Epsilon)
		fmt.Println("Sigurnost tacnosti: ", activeCMS.Delta)
		fmt.Println("Broj hash funkcija (dubina): ", activeCMS.K)
		fmt.Println("Broj kolona (sirina): ", activeCMS.M)
		fmt.Println()
		fmt.Println("1 - Kreiraj CountMinSketch")
		fmt.Println("2 - Dobavi CountMinSketch iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Proveri broj ponavljanja")
		fmt.Println("5 - Upisi CountMinSketch u bazu podataka")
		fmt.Println("6 - Obrisi CountMinSketch iz baze podataka")
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
			found, tempKey, tempCms := CreateCountMinSketch(mem, lru, bucket)
			if !found {
				activeCMS = tempCms
				activeKey = tempKey
				userkey = activeKey[14:]
			}
			
		case "2":
			found, key, tempCMS := CountMinSketchGET(mem, lru, bucket)
			if key != "*" {
				if found {
					activeCMS = tempCMS
					activeKey = key
					userkey = activeKey[14:]
					fmt.Println("Uspesno dobavljanje")
				} else {
					fmt.Println("Ne postoji CountMinSKetch sa datim kljucem")
				}
			}

		case "3":

			if len(activeKey) != 0 {
				CountMinSketchAddElement(activeCMS)
			} else{
				fmt.Println("Nije izabran aktivni CMS")
			}

		case "4":
			if len(activeKey) != 0 {
				CountMinSketchCheckFrequency(activeCMS)
			} else{
				fmt.Println("Nije izabran aktivni CMS")
			}
		case "5":
			if len(activeKey) != 0 {
				CountMinSketchPUT(activeKey, activeCMS, mem, bucket)
			} else{
				fmt.Println("Nije izabran aktivni CMS")
			}
		case "6":
			if len(activeKey) != 0 {
				CountMinSketchDELETE(activeKey, mem, lru, bucket)
				fmt.Println("Uspesno brisanje")
			} else{
				fmt.Println("Nije izabran aktivni CMS")
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