package sstable

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	. "project/keyvalue/config"
	. "project/keyvalue/structures/bloom"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/entry"
	merkle "project/keyvalue/structures/merkle"
	. "project/keyvalue/structures/scan"
	"strconv"
	"strings"
)

type SSTableMulti struct {
	intervalSize uint
	directory    string
	bloomFilter  *BloomFilter
}

// ---------------- Konstruktor i inicijalizacija ----------------

// size - ocekivani broj elemenata (velicina memtabele)
// directory - naziv direktorijuma
func NewSSTableMulti(size uint32, directory string) *SSTableMulti {
	config := GetConfig()
	sstable := new(SSTableMulti)
	sstable.intervalSize = config.SStableInterval
	sstable.directory = directory

	_, err := os.Stat("files/sstable/" + sstable.directory)
	if os.IsNotExist(err) {
		sstable.bloomFilter = NewBloomFilter(size, config.BloomFalsePositiveRate)
	} else {
		sstable.LoadFilter()
	}

	return sstable
}

// Otvara trazenu datoteku od sstabele
func (sstable *SSTableMulti) OpenFile(filename string) *os.File {
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	file, err := os.Open(path + "/" + filename)
	if err != nil {
		log.Fatal(err)
	}

	return file
}

// Ucitava podatke ukoliko vec postoji sstabela
func (sstable *SSTableMulti) LoadFilter() {
	//Ucitavamo bloomfilter
	filterFile := sstable.OpenFile("filter.bin")
	sstable.bloomFilter = ByteToBloomFilter(filterFile)
	err := filterFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// Vraca pokazivace na kreirane fajlove(summary,index,data, filter, metadata)
func (sstable *SSTableMulti) MakeFiles() []*os.File {
	//kreiramo novi direktorijum
	_, err := os.Stat("files/sstable/" + sstable.directory)
	if os.IsNotExist(err) {
		err = os.MkdirAll("files/sstable/"+sstable.directory, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("Fajl vec postoji!")
	}

	//Kreiramo fajlove unutar direktorijuma
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	summary, err3 := os.Create(path + "/summary.bin")
	if err3 != nil {
		log.Fatal(err3)
	}

	index, err4 := os.Create(path + "/index.bin")
	if err4 != nil {
		log.Fatal(err4)
	}

	data, err5 := os.Create(path + "/data.bin")
	if err5 != nil {
		log.Fatal(err5)
	}

	filter, err6 := os.Create(path + "/filter.bin")
	if err6 != nil {
		log.Fatal(err6)
	}

	metadata, err7 := os.Create(path + "/metadata.txt")
	if err7 != nil {
		log.Fatal(err7)
	}

	files := make([]*os.File, 0)
	files = append(files, data, index, summary, filter, metadata)
	return files
}

// Iterira se kroz string kljuceve i ubacuje u:
// Bloomfilter
// zapisuje u data, index tabelu, summary
func (sstable *SSTableMulti) Flush(keys []string, values []*Data) {
	files := sstable.MakeFiles()
	dataFile, indexFile, summaryFile, filterFile, metadataFile := files[0], files[1], files[2], files[3], files[4]
	summary := new(Summary)
	summary.FirstKey = keys[0]
	summary.LastKey = keys[len(keys)-1]
	summary.Intervals = make([]*Index, 0)

	offsetIndex := uint64(0) //Offset ka indeksu(koristi se u summary)
	offsetData := uint64(0)  //Offset ka disku(koristi se u indeks tabeli)

	nodes := make([]*merkle.Node, 0) //

	intervalCounter := uint(sstable.intervalSize) //Kada dostigne postavljeni interval zapisuje novi Offset indeksnog intervala
	for i := 0; i < len(keys); i++ {
		index := new(Index) //Pomocna struktura (menja se u svakoj iteraciji)

		//Dodajemo u bloomFilter
		sstable.bloomFilter.AddToBloom([]byte(keys[i]))

		//Dodajemo u merkle
		node := new(merkle.Node)
		node.Data = dataToByte(keys[i], values[i])
		nodes = append(nodes, node)

		//Upisujemo trenutni podatak u data tabelu
		dataLen, err1 := dataFile.Write(dataToByte(keys[i], values[i]))
		if err1 != nil {
			log.Fatal(err1)
		}

		//upisujemo trenutni podatak u indeks tabelu
		index.Key = keys[i]
		index.KeySize = uint32(len([]byte(index.Key)))
		index.Offset = offsetData
		indexLen, err := indexFile.Write(indexToByte(index))
		if err != nil {
			log.Fatal(err)
		}

		if intervalCounter == sstable.intervalSize {
			index.Offset = offsetIndex

			//Ubacimo u summary
			summary.Intervals = append(summary.Intervals, index)

			intervalCounter = 0
		}

		offsetData += uint64(dataLen)
		offsetIndex += uint64(indexLen)
		intervalCounter++
	}

	//Upis summary u summaryFile
	_, err2 := summaryFile.Write(summaryToByte(summary))
	if err2 != nil {
		log.Fatal(err2)
	}

	//Upis u bloomfilter fajl
	_, err := filterFile.Write(BloomFilterToByte(sstable.bloomFilter))
	if err != nil {
		log.Fatal(err)
	}

	//Upis u metadata fajl
	merkleRoot := merkle.MakeMerkel(nodes)
	merkle.WriteFile(metadataFile, merkleRoot.Root)

	//Zatvaranje fajlova
	err = summaryFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = indexFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = dataFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = filterFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = metadataFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// ------------ PRINTOVANJE ------------

func (sstable *SSTableMulti) ReadData() {
	file := sstable.OpenFile("data.bin")

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		entry.Print()
	}
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (sstable *SSTableMulti) ReadIndex() {
	file := sstable.OpenFile("index.bin")

	for {
		index := byteToIndex(file)
		if index == nil {
			break
		}
		fmt.Println(index)
	}
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (sstable *SSTableMulti) ReadSummary() *Summary {
	file := sstable.OpenFile("summary.bin")

	summary := byteToSummary(file)

	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}

	return summary
}

func (sstable *SSTableMulti) ReadBloom() {
	file := sstable.OpenFile("filter.bin")

	blm := ByteToBloomFilter(file)
	fmt.Println("K: ", blm.K)
	fmt.Println("N: ", blm.N)
	fmt.Println("M: ", blm.M)
	fmt.Println("Bitset: ", blm.Bitset)
	fmt.Println("hashfuncs: ", blm.HashFuncs)
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}

}

// ------------ PRETRAZIVANJE ------------

func (sstable *SSTableMulti) Find(Key string) (bool, *Data) {
	//Ucitavamo bloomfilter
	filterFile := sstable.OpenFile("filter.bin")
	sstable.bloomFilter = ByteToBloomFilter(filterFile)
	err := filterFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	//Proveravamo preko BloomFiltera da li uopste treba da pretrazujemo
	if !sstable.bloomFilter.IsInBloom([]byte(Key)) {
		return false, nil
	}

	//Proveravamo da li je kljuc van opsega
	summary := sstable.ReadSummary()

	if Key < summary.FirstKey || Key > summary.LastKey {
		return false, nil
	}

	indexInSummary := new(Index)
	found := false
	for i := 1; i < len(summary.Intervals); i++ {
		if Key < summary.Intervals[i].Key {
			indexInSummary = summary.Intervals[i-1]
			found = true
			break
		}
	}
	if !found {
		indexInSummary = summary.Intervals[len(summary.Intervals)-1]
	}

	// ------ Otvaramo index tabelu ------
	indexFile := sstable.OpenFile("index.bin")

	found = false
	_, err = indexFile.Seek(int64(indexInSummary.Offset), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
	if err != nil {
		log.Fatal(err)
	}
	currentIndex := new(Index)

	//trazimo redom
	for i := 0; i < int(sstable.intervalSize); i++ {
		currentIndex = byteToIndex(indexFile)
		if currentIndex.Key == Key {
			found = true
			break
		}
	}
	err = indexFile.Close() //zatvaramo indeksnu tabelu
	if err != nil {
		log.Fatal(err)
	}

	if !found {
		return false, nil
	}

	// ------ Pristupamo disku i uzimamo podtak ------
	dataFile := sstable.OpenFile("data.bin")
	_, foundData := ByteToData(dataFile, currentIndex.Offset)
	err = dataFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	return true, foundData
}

// ------------ DOBAVLJANJE PODATAKA ------------

// Otvara fajl i postavlja pokazivac na pocetak data zone
// vraca pokazivac na taj fajl i velicinu data zone
func (sstable *SSTableMulti) GoToData() (*os.File, uint64) {
	file := sstable.OpenFile("data.bin")
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return file, uint64(fileInfo.Size())
}

// ------------- RANGE SCAN -------------
// Prolazi kroz sstabelu i trazi kljuceve koji zadovoljavaju trazeni interval
func (sstable *SSTableMulti) RangeScan(minKey string, maxKey string, scan *Scan) {

	//Proveravamo da li je kljuc van opsega
	summary := sstable.ReadSummary()

	if maxKey < summary.FirstKey || minKey > summary.LastKey {
		return //Preskacemo ovu sstabelu jer kljucevi nisu u opsegu
	}

	chosenIntervals := make([]*Index, 0)
	for i := 1; i < len(summary.Intervals); i++ {
		if summary.Intervals[i].Key < minKey {
			continue
		}
		if maxKey < summary.Intervals[i-1].Key {
			break
		}
		chosenIntervals = append(chosenIntervals, summary.Intervals[i-1])
	}

	if len(chosenIntervals) < 1 {
		return
	}

	// ------ Otvaramo index tabelu ------
	indexFile := sstable.OpenFile("index.bin")
	currentIndex := new(Index)

	dataFile := sstable.OpenFile("data.bin") //Otvaramo data fajl za proveru

	//Prolazimo kroz sve nadjene indeksne delove
	for i := 0; i < len(chosenIntervals); i++ {
		if scan.FoundResults > scan.SelectedPageEnd {
			break
		}

		_, err := indexFile.Seek(int64(chosenIntervals[i].Offset), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
		if err != nil {
			log.Fatal(err)
		}

		//trazimo redom
		for i := 0; i < int(sstable.intervalSize); i++ {
			currentIndex = byteToIndex(indexFile)
			if currentIndex.Key >= minKey && currentIndex.Key <= maxKey {

				// -------- pristupamo disku i proveravamo podatak --------
				foundKey, foundData := ByteToData(dataFile, currentIndex.Offset)
				if !foundData.Tombstone {
					//Proveravamo da li je obelezen kao obrisan ili je vec dodat
					if !scan.RemovedKeys[foundKey] && !scan.SelectedKeys[foundKey] {
						scan.SelectedKeys[foundKey] = true //Obelezimo da je dodat

						scan.FoundResults++
						//Ukoliko je u opsegu nase stranice pamtimo u Scan
						if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd {
							scan.Keys = append(scan.Keys, foundKey)
							scan.Data = append(scan.Data, foundData)
						} else if scan.FoundResults > scan.SelectedPageEnd {
							break
						}
					}
				} else {
					//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
					scan.RemovedKeys[foundKey] = true
				}
			} else if currentIndex.Key > maxKey {
				break
			}
		}
	}

	err := indexFile.Close() //zatvaramo indeksnu tabelu
	if err != nil {
		log.Fatal(err)
	}

	err = dataFile.Close() //Zatvaramo data zonu
	if err != nil {
		log.Fatal(err)
	}

}

// ------------- LIST SCAN -------------
// Prolazi kroz sstabelu i trazi kljuceve koji pocinju zadatim prefiksom
func (sstable *SSTableMulti) ListScan(prefix string, scan *Scan) {

	//Proveravamo da li je kljuc van opsega
	summary := sstable.ReadSummary()

	//najmanje duzine stringova
	//Trazimo koji string je manji i onda proveravamo toliko cifara, da ne bi izasli iz index range-a
	minimumLenFirst := int(math.Min(float64(len(prefix)), float64(len(summary.FirstKey))))
	minimumLenLast := int(math.Min(float64(len(prefix)), float64(len(summary.LastKey))))

	if prefix[:minimumLenFirst] < summary.FirstKey[:minimumLenFirst] || prefix[:minimumLenLast] > summary.LastKey[:minimumLenLast] {
		return //Preskacemo ovu sstabelu jer kljucevi nisu u opsegu
	}

	//Biramo koji indeksni intervali nam trebaju
	chosenIntervals := make([]*Index, 0)
	for i := 1; i < len(summary.Intervals); i++ {
		//Trazimo koji string je manji i onda proveravamo toliko cifara, da ne bi izasli iz index range-a
		//za trenutan interval
		minimumLen := int(math.Min(float64(len(prefix)), float64(len(summary.Intervals[i].Key))))
		if summary.Intervals[i].Key[:minimumLen] < prefix[:minimumLen] {
			continue
		}
		//za prethodni interval
		minimumLen = int(math.Min(float64(len(prefix)), float64(len(summary.Intervals[i-1].Key))))
		if prefix[:minimumLen] < summary.Intervals[i-1].Key[:minimumLen] {
			break
		}
		chosenIntervals = append(chosenIntervals, summary.Intervals[i-1])
	}

	if len(chosenIntervals) < 1 {
		return
	}

	// ------ Otvaramo index tabelu ------
	indexFile := sstable.OpenFile("index.bin")
	currentIndex := new(Index)

	dataFile := sstable.OpenFile("data.bin") //Otvaramo data fajl za proveru

	//Prolazimo kroz sve nadjene indeksne delove
	for i := 0; i < len(chosenIntervals); i++ {
		if scan.FoundResults > scan.SelectedPageEnd {
			break
		}

		_, err := indexFile.Seek(int64(chosenIntervals[i].Offset), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
		if err != nil {
			log.Fatal(err)
		}

		//trazimo redom
		for i := 0; i < int(sstable.intervalSize); i++ {
			currentIndex = byteToIndex(indexFile)
			if strings.HasPrefix(currentIndex.Key, prefix) {
				// -------- pristupamo disku i proveravamo podatak --------
				foundKey, foundData := ByteToData(dataFile, currentIndex.Offset)
				if !foundData.Tombstone {
					//Proveravamo da li je obelezen kao obrisan ili je vec dodat
					if !scan.RemovedKeys[foundKey] && !scan.SelectedKeys[foundKey] {
						scan.SelectedKeys[foundKey] = true //Obelezimo da je dodat

						scan.FoundResults++
						//Ukoliko je u opsegu nase stranice pamtimo u Scan
						if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd {
							scan.Keys = append(scan.Keys, foundKey)
							scan.Data = append(scan.Data, foundData)
						} else if scan.FoundResults > scan.SelectedPageEnd {
							break
						}
					}
				} else {
					//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
					scan.RemovedKeys[foundKey] = true
				}
			} else if currentIndex.Key > prefix {
				break
			}
		}
	}

	err := indexFile.Close() //zatvaramo indeksnu tabelu
	if err != nil {
		log.Fatal(err)
	}

	err = dataFile.Close() //Zatvaramo data zonu
	if err != nil {
		log.Fatal(err)
	}

}

// Vraca koji je nivo i koja je po redu sstabela u LSM stablu
func (sstable *SSTableMulti) GetPosition() (uint32, uint32) {
	arr := strings.Split(sstable.directory, "/")
	levelString := strings.TrimLeft(arr[0], "level")
	fileString := strings.TrimLeft(arr[1], "sstable")

	levelNum, err := strconv.Atoi(levelString)
	if err != nil {
		log.Fatal(err)
	}

	fileNum, err := strconv.Atoi(fileString)
	if err != nil {
		log.Fatal(err)
	}

	return uint32(levelNum), uint32(fileNum)
}

//Vraca opseg iz summaryja
func (sstable *SSTableMulti) GetRange() (string, string) {
	summary := sstable.ReadSummary()
	return summary.FirstKey, summary.LastKey
}
