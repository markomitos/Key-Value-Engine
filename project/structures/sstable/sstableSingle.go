package sstable

import (
	"encoding/binary"
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

type SSTableSingle struct {
	intervalSize uint
	directory    string
	bloomFilter  *BloomFilter
}

// Otvara trazenu datoteku od sstabele
func (sstable *SSTableSingle) OpenFile(filename string) *os.File {
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

func NewSSTableSingle(size uint32, directory string) *SSTableSingle {
	config := GetConfig()
	sstable := new(SSTableSingle)
	sstable.intervalSize = config.SStableInterval
	sstable.directory = directory

	_, err := os.Stat("files/sstable/" + sstable.directory)
	if os.IsNotExist(err) {
		sstable.bloomFilter = NewBloomFilter(size, config.BloomFalsePositiveRate)
	} else {
		sstable.LoadFilter() //Treba popraviti i videti da li je potrebno uopste
	}

	return sstable
}

func (sstable *SSTableSingle) LoadFilter() {
	//Otvaramo fajl i citamo header
	sstableFile := sstable.OpenFile("sstable.bin")

	//Citamo velicinu data zone
	bytes := make([]byte, 8)
	_, err := sstableFile.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	dataSize := binary.BigEndian.Uint64(bytes)

	//Citamo velicinu indeksne zone
	bytes = make([]byte, 8)
	_, err = sstableFile.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	indexSize := binary.BigEndian.Uint64(bytes)

	//Citamo velicinu summary zone
	bytes = make([]byte, 8)
	_, err = sstableFile.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	summarySize := binary.BigEndian.Uint64(bytes)

	//Offseti na pocetke zona
	dataStart := uint64(24)
	indexStart := dataStart + dataSize
	summaryStart := indexStart + indexSize
	filterStart := summaryStart + summarySize

	//Ucitavamo bloomfilter
	_, err = sstableFile.Seek(int64(filterStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	sstable.bloomFilter = ByteToBloomFilter(sstableFile)

	err = sstableFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// Vraca pokazivace na kreirane fajlove(summary,index,data, filter, metadata)
func (sstable *SSTableSingle) MakeFiles() []*os.File {
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

	sstableFile, err3 := os.Create(path + "/sstable.bin")
	if err3 != nil {
		log.Fatal(err3)
	}

	metadata, err7 := os.Create(path + "/metadata.txt")
	if err7 != nil {
		log.Fatal(err7)
	}

	files := make([]*os.File, 0)
	files = append(files, sstableFile, metadata)
	return files
}

// Iterira se kroz string kljuceve i ubacuje u:
// Bloomfilter
// zapisuje u data, index tabelu, summary
func (sstable *SSTableSingle) Flush(keys []string, values []*Data) {
	files := sstable.MakeFiles()
	sstableFile, metadataFile := files[0], files[1]
	summary := new(Summary)
	summary.FirstKey = keys[0]
	summary.LastKey = keys[len(keys)-1]
	summary.Intervals = make([]*Index, 0)

	offsetIndex := uint64(0) //Relativan offset ka indeksu(koristi se u summary)
	offsetData := uint64(0)  //Relativan offset ka disku(koristi se u indeks tabeli)

	nodes := make([]*merkle.Node, 0)

	//Cuva podatke o data zoni
	dataBytes := make([]byte, 0)

	//Cuva podatke o index zoni
	indexBytes := make([]byte, 0)

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
		tempData := dataToByte(keys[i], values[i])
		dataBytes = append(dataBytes, tempData...)
		dataLen := len(tempData)

		//upisujemo trenutni podatak u indeks tabelu
		index.Key = keys[i]
		index.KeySize = uint32(len([]byte(index.Key)))
		index.Offset = offsetData
		tempIndex := indexToByte(index)
		indexBytes = append(indexBytes, tempIndex...)
		indexLen := len(tempIndex)

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

	summaryBytes := summaryToByte(summary)

	// ------------ HEADER ------------
	// --> 3*uint64 = 24B

	// 1. element je velicina data zone
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(len(dataBytes)))

	// 2. element je velicina indeksne zone
	bytesTemp := make([]byte, 8)
	binary.BigEndian.PutUint64(bytesTemp, uint64(len(indexBytes)))
	bytes = append(bytes, bytesTemp...)

	// 3. element je velicina summary zone
	bytesTemp = make([]byte, 8)
	binary.BigEndian.PutUint64(bytesTemp, uint64(len(summaryBytes)))
	bytes = append(bytes, bytesTemp...)

	//Upisujemo header u fajl
	_, err := sstableFile.Write(bytes)
	if err != nil {
		log.Fatal(err)
	}

	//------------ DATA ------------
	_, err = sstableFile.Write(dataBytes)
	if err != nil {
		log.Fatal(err)
	}

	//------------ INDEX ------------
	_, err = sstableFile.Write(indexBytes)
	if err != nil {
		log.Fatal(err)
	}

	//------------ SUMMARY ------------
	_, err = sstableFile.Write(summaryBytes)
	if err != nil {
		log.Fatal(err)
	}

	//------------ FILTER ------------
	_, err = sstableFile.Write(BloomFilterToByte(sstable.bloomFilter))
	if err != nil {
		log.Fatal(err)
	}

	//Upis u metadata fajl
	merkleRoot := merkle.MakeMerkel(nodes)
	merkle.WriteFile(metadataFile, merkleRoot.Root)

	//Zatvaranje fajlova
	err = sstableFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = metadataFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (sstable *SSTableSingle) Find(Key string) (bool, *Data) {

	//Otvaramo fajl i citamo header
	sstableFile := sstable.OpenFile("sstable.bin")

	dataSize, indexSize, summarySize := sstable.ReadHeader(sstableFile)

	//Offseti na pocetke zona
	dataStart := uint64(24)
	indexStart := dataStart + dataSize
	summaryStart := indexStart + indexSize
	filterStart := summaryStart + summarySize

	//Ucitavamo bloomfilter
	_, err := sstableFile.Seek(int64(filterStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	sstable.bloomFilter = ByteToBloomFilter(sstableFile)

	//Proveravamo preko BloomFiltera da li uopste treba da pretrazujemo
	if !sstable.bloomFilter.IsInBloom([]byte(Key)) {
		err := sstableFile.Close()
		if err != nil {
			log.Fatal(err)
		}
		return false, nil
	}

	//Proveravamo da li je kljuc van opsega
	_, err = sstableFile.Seek(int64(summaryStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	summary := byteToSummary(sstableFile)

	if Key < summary.FirstKey || Key > summary.LastKey {
		err = sstableFile.Close()
		if err != nil {
			log.Fatal(err)
		}
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
	found = false
	_, err = sstableFile.Seek(int64(indexInSummary.Offset+indexStart), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
	if err != nil {
		log.Fatal(err)
	}
	currentIndex := new(Index)

	//trazimo redom
	for i := 0; i < int(sstable.intervalSize); i++ {
		currentIndex = byteToIndex(sstableFile)
		if currentIndex.Key == Key {
			found = true
			break
		}
	}

	if !found {
		err := sstableFile.Close()
		if err != nil {
			log.Fatal(err)
		}
		return false, nil
	}

	// ------ Pristupamo disku i uzimamo podatak ------
	_, err = sstableFile.Seek(int64(currentIndex.Offset+dataStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	_, foundData := ByteToData(sstableFile)

	err = sstableFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	return true, foundData
}

// Vraca duzinu data,index,summary
func (sstable *SSTableSingle) ReadHeader(file *os.File) (uint64, uint64, uint64) {
	//Citamo velicinu data zone
	bytes := make([]byte, 8)
	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	dataSize := binary.BigEndian.Uint64(bytes)

	//Citamo velicinu indeksne zone
	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	indexSize := binary.BigEndian.Uint64(bytes)

	//Citamo velicinu summary zone
	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	summarySize := binary.BigEndian.Uint64(bytes)

	return dataSize, indexSize, summarySize
}

// Otvara fajl i postavlja pokazivac na pocetak data zone
// vraca pokazivac na taj fajl i velicinu data zone
func (sstable *SSTableSingle) GoToData() (*os.File, uint64) {
	file := sstable.OpenFile("sstable.bin")

	//Citamo header
	dataSize, _, _ := sstable.ReadHeader(file)

	return file, dataSize + 24
}

// ------------- RANGE SCAN -------------
// Prolazi kroz sstabelu i trazi kljuceve koji zadovoljavaju trazeni interval
func (sstable *SSTableSingle) RangeScan(minKey string, maxKey string, scan *Scan) {

	//Otvaramo fajl i citamo header
	sstableFile := sstable.OpenFile("sstable.bin")

	dataSize, indexSize, _ := sstable.ReadHeader(sstableFile)

	//Offseti na pocetke zona
	dataStart := uint64(24)
	indexStart := dataStart + dataSize
	summaryStart := indexStart + indexSize

	//Proveravamo da li je kljuc van opsega
	_, err := sstableFile.Seek(int64(summaryStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	summary := byteToSummary(sstableFile)

	if maxKey < summary.FirstKey || minKey > summary.LastKey {
		err := sstableFile.Close()
		if err != nil {
			log.Fatal(err)
		}
		return //Preskacemo ovu sstabelu jer kljucevi nisu u opsegu
	}

	chosenIntervals := make([]*Index, 0) //Cuva intervale koji treba da se pregledaju
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
	currentIndex := new(Index)

	//Prolazimo kroz sve nadjene indeksne delove
	for i := 0; i < len(chosenIntervals); i++ {
		if scan.FoundResults > scan.SelectedPageEnd {
			break
		}

		_, err := sstableFile.Seek(int64(chosenIntervals[i].Offset+indexStart), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
		if err != nil {
			log.Fatal(err)
		}

		//lista offseta na podatke koji treba da se provere
		dataOffsetToCheck := make([]uint64, 0)

		//Dodajemo indekse u listu
		for i := 0; i < int(sstable.intervalSize); i++ {
			currentIndex = byteToIndex(sstableFile)
			if currentIndex.Key >= minKey && currentIndex.Key <= maxKey {
				dataOffsetToCheck = append(dataOffsetToCheck, currentIndex.Offset)
			} else if currentIndex.Key > maxKey {
				break
			}
		}

		// ------ Pristupamo disku i uzimamo podatake ------

		//Prolazimo kroz svaki indeks i trazimo koji nam trebaju
		for i := 0; i < len(dataOffsetToCheck); i++ {
			//Pozicioniramo se na podatak i citamo ga
			foundKey, foundData := ByteToData(sstableFile, dataOffsetToCheck[i]+dataStart)
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

		}
	}

	err = sstableFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// ------------- LIST SCAN -------------
// Prolazi kroz sstabelu i trazi kljuceve koji pocinju zadatim prefiksom
func (sstable *SSTableSingle) ListScan(prefix string, scan *Scan) {

	//Otvaramo fajl i citamo header
	sstableFile := sstable.OpenFile("sstable.bin")

	dataSize, indexSize, _ := sstable.ReadHeader(sstableFile)

	//Offseti na pocetke zona
	dataStart := uint64(24)
	indexStart := dataStart + dataSize
	summaryStart := indexStart + indexSize

	//Proveravamo da li je kljuc van opsega
	_, err := sstableFile.Seek(int64(summaryStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	summary := byteToSummary(sstableFile)

	//najmanje duzine stringova
	//Trazimo koji string je manji i onda proveravamo toliko cifara, da ne bi izasli iz index range-a
	minimumLenFirst := int(math.Min(float64(len(prefix)), float64(len(summary.FirstKey))))
	minimumLenLast := int(math.Min(float64(len(prefix)), float64(len(summary.LastKey))))

	if prefix[:minimumLenFirst] < summary.FirstKey[:minimumLenFirst] || prefix[:minimumLenLast] > summary.LastKey[:minimumLenLast] {
		err := sstableFile.Close()
		if err != nil {
			log.Fatal(err)
		}
		return //Preskacemo ovu sstabelu jer kljucevi nisu u opsegu
	}

	chosenIntervals := make([]*Index, 0) //Cuva intervale koji treba da se pregledaju
	for i := 1; i < len(summary.Intervals); i++ {

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
	currentIndex := new(Index)

	//Prolazimo kroz sve nadjene indeksne delove
	for i := 0; i < len(chosenIntervals); i++ {
		if scan.FoundResults > scan.SelectedPageEnd {
			break
		}

		_, err := sstableFile.Seek(int64(chosenIntervals[i].Offset+indexStart), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
		if err != nil {
			log.Fatal(err)
		}

		//lista offseta na podatke koji treba da se provere
		dataOffsetToCheck := make([]uint64, 0)

		//Dodajemo indekse u listu
		for i := 0; i < int(sstable.intervalSize); i++ {
			currentIndex = byteToIndex(sstableFile)
			if strings.HasPrefix(currentIndex.Key, prefix) {
				dataOffsetToCheck = append(dataOffsetToCheck, currentIndex.Offset)
			} else if currentIndex.Key > prefix {
				break
			}
		}

		// ------ Pristupamo disku i uzimamo podatake ------

		//Prolazimo kroz svaki indeks i trazimo koji nam trebaju
		for i := 0; i < len(dataOffsetToCheck); i++ {
			//Pozicioniramo se na podatak i citamo ga
			foundKey, foundData := ByteToData(sstableFile, dataOffsetToCheck[i]+dataStart)
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

		}
	}

	err = sstableFile.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (sstable *SSTableSingle) ReadData() {
	file := sstable.OpenFile("sstable.bin")
	dataSize, _, _ := sstable.ReadHeader(file)

	//Offseti na pocetke zona
	dataStart := uint64(24)
	indexStart := dataStart + dataSize

	for {
		currentOffset, err := file.Seek(0, 1)
		if err != nil {
			log.Fatal(err)
		}
		if uint64(currentOffset) >= indexStart {
			break
		}
		entry := ReadEntry(file)
		entry.Print()
	}
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// Vraca koji je nivo i koja je po redu sstabela u LSM stablu
func (sstable *SSTableSingle) GetPosition() (uint32, uint32) {
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
func (sstable *SSTableSingle) GetRange() (string, string) {
	//Otvaramo fajl i citamo header
	sstableFile := sstable.OpenFile("sstable.bin")

	dataSize, indexSize, _ := sstable.ReadHeader(sstableFile)

	//Offseti na pocetke zona
	dataStart := uint64(24)
	indexStart := dataStart + dataSize
	summaryStart := indexStart + indexSize

	//Proveravamo da li je kljuc van opsega
	_, err := sstableFile.Seek(int64(summaryStart), 0)
	if err != nil {
		log.Fatal(err)
	}
	summary := byteToSummary(sstableFile)

	err = sstableFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	return summary.FirstKey, summary.LastKey
}
