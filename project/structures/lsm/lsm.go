package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	. "project/keyvalue/config"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/scan"
	. "project/keyvalue/structures/sstable"
	"strconv"
	"time"
)

//Ovde organizujemo fajlove pri upisu

type Lsm struct {
	MaxLevel   uint32
	Level      uint32   //Trenutna visina
	LevelSizes []uint32 //cuva broj sstabela u svakom nivou
}

// Kreira foldere i lsm fajl ako ne postoji
func InitializeLsm() {
	_, err := os.Stat("files/sstable/lsm.bin")
	if os.IsNotExist(err) {
		config := GetConfig()
		lsm := new(Lsm)
		lsm.MaxLevel = uint32(config.LsmMaxLevel)
		lsm.Level = 1
		lsm.LevelSizes = make([]uint32, lsm.MaxLevel)

		path, err := filepath.Abs("files/sstable")
		if err != nil {
			log.Fatal(err)
		}
		file, err := os.Create(path + "/lsm.bin")
		if err != nil {
			log.Fatal(err)
		}
		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}

		lsm.Write()
		lsm.GenerateLevelFolders()
	} else {
		//Ukoliko je maxlevel veci od broja trenutnih foldera kreirace se novi
		ReadLsm().GenerateLevelFolders()
	}
}

// Zapisuje lsm u fajl
func (lsm *Lsm) Write() {
	filePath, err1 := filepath.Abs("files/sstable/lsm.bin")
	if err1 != nil {
		log.Fatal(err1)
	}
	file, err := os.OpenFile(filePath, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}

	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, lsm.MaxLevel)

	tempBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(tempBytes, lsm.Level)
	bytes = append(bytes, tempBytes...)

	for i := 0; i < len(lsm.LevelSizes); i++ {
		tempBytes = make([]byte, 4)
		binary.BigEndian.PutUint32(tempBytes, lsm.LevelSizes[i])
		bytes = append(bytes, tempBytes...)
	}

	_, err = file.Write(bytes)
	if err != nil {
		log.Fatal(err)
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// Ucitava LSM sa diska
func ReadLsm() *Lsm {
	filePath, err1 := filepath.Abs("files/sstable/lsm.bin")
	if err1 != nil {
		log.Fatal(err1)
	}
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	lsm := new(Lsm)

	bytes := make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	lsm.MaxLevel = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	lsm.Level = binary.BigEndian.Uint32(bytes)

	for true {
		bytes = make([]byte, 4)
		_, err = file.Read(bytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		lsm.LevelSizes = append(lsm.LevelSizes, binary.BigEndian.Uint32(bytes))
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
	return lsm
}

// Imenuje sstabelu nakon flusha
func GenerateFlushName() string {
	lsm := ReadLsm()
	currentMax := lsm.LevelSizes[0]
	return "level1/sstable" + strconv.FormatUint(uint64(currentMax+1), 10)
}

//Vraca ime za sstabelu za zadati nivo i indeks
func (lsm *Lsm) GenerateSSTableName(currentLevel uint32, index uint32) string {
	return "level" + strconv.FormatUint(uint64(currentLevel), 10) + "/sstable" + strconv.FormatUint(uint64(index), 10)
}

// Pokrece se pri upisu nove sstabele
// Povecava trenutni broj za 1 u levelu
func IncreaseLsmLevel(level uint32) {
	lsm := ReadLsm()
	lsm.LevelSizes[level-1]++
	lsm.Write()
}

// Menja imena fajlova tako da krecu od 1
// Update-a velicinu levela
func (lsm *Lsm) RenameLevelSizeTiered(level uint32) {
	if lsm.LevelSizes[level-1]%2 != 0 {

		err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(level), 10)+"/sstable"+strconv.FormatUint(uint64(lsm.LevelSizes[level-1]), 10),
			"files/sstable/level"+strconv.FormatUint(uint64(level), 10)+"/sstable1")
		if err != nil {
			log.Fatal(err)
		}
		lsm.LevelSizes[level-1] = 1
	} else {
		lsm.LevelSizes[level-1] = 0
	}
}

// Menja imena fajlova tako da krecu od 1
func (lsm *Lsm) RenameLevelLeveled(currentLevel uint32, numOfCreatedFiles uint32, chosenIndexes []uint32) {
	config := GetConfig()

	//Ovaj slucaj gledamo ako postoje preklapanja sa narednim nivoom
	if len(chosenIndexes) > 0 {
		//Broj elemenata koji se nalaze izmedju novododatih posle kompakcije i odabranih za kompakciju
		middle := lsm.LevelSizes[currentLevel-1] - chosenIndexes[len(chosenIndexes)-1] - numOfCreatedFiles

		//Pomeramo middle skroz desno iza novododatih(preimenujemo ih)
		renameCnt := uint32(1)
		for i := chosenIndexes[len(chosenIndexes)-1] + 1; i <= lsm.LevelSizes[currentLevel-1]-numOfCreatedFiles; i++ {
			err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i), 10),
				"files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(lsm.LevelSizes[currentLevel-1]+renameCnt), 10)) //Najkraca linija koda u Novom Sadu
			if err != nil {
				log.Fatal(err)
			}
			renameCnt++
		}

		//Pomeramo sve pocev od novododatih u levo preko onih koje smo obrisali(koji su se koristili u kompakciji)
		for i := chosenIndexes[len(chosenIndexes)-1] + middle + 1; i <= lsm.LevelSizes[currentLevel-1]+middle; i++ {
			err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i), 10),
				"files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i-uint32(len(chosenIndexes))-middle), 10))
			if err != nil {
				log.Fatal(err)
			}
		}
		lsm.LevelSizes[currentLevel-1] -= uint32(len(chosenIndexes))

	} else { //Ukoliko nema preklapanja
		indexOfFirstCreated := lsm.LevelSizes[currentLevel-1] - numOfCreatedFiles + 1
		lastCreatedSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, lsm.LevelSizes[currentLevel-1]))

		_, maxCreated := lastCreatedSSTable.GetRange()

		swapIndex := uint32(0) //Predstavlja indeks gde treba da zamenimo tabele

		//Poredimo opseg ostalih sstabela sa dodatim tabelama
		//da bi znali na kojoj poziciji treba da stavimo dodate sstabele da bi sve bile sortirane
		for i := uint32(1); i < indexOfFirstCreated; i++ {
			currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, i))
			minCurrent, _ := currentSSTable.GetRange()

			if maxCreated < minCurrent {
				swapIndex = i
				break
			}
		}

		//Ovo znaci da nije nasao nigde mesto tj. treba da stoji na kraju i nista ne pomeramo
		if swapIndex == 0 {
			return
		}

		middleCounter := uint32(0) //Broji koliko tabela ima izmedju prvog dodatog i mesta gde treba da ubacimo

		//Pomeramo na kraj sve tabele koje se nalaze izmedju
		for i := swapIndex; i < indexOfFirstCreated; i++ {
			err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i), 10),
				"files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(lsm.LevelSizes[currentLevel-1]+middleCounter+1), 10))
			if err != nil {
				log.Fatal(err)
			}
			middleCounter++
		}

		//Pomeramo sve u levo na trazeno mesto pocev od prvog dodatog tako da sada sve bude sortirano
		for i := indexOfFirstCreated; i <= lsm.LevelSizes[currentLevel-1]+middleCounter; i++ {
			err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i), 10),
				"files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i-middleCounter), 10))
			if err != nil {
				log.Fatal(err)
			}
		}

	}

}

// Preostale fajlove nakon kompakcije preimenuje da pocinju od 1
func (lsm *Lsm) UpdateCurrentLevelNames(currentLevel uint32, numOfCompacted uint32) {

	//Pomeramo sve u levo na pocetak
	for i := numOfCompacted + 1; i <= lsm.LevelSizes[currentLevel-1]; i++ {
		err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i), 10),
			"files/sstable/level"+strconv.FormatUint(uint64(currentLevel), 10)+"/sstable"+strconv.FormatUint(uint64(i-numOfCompacted), 10))
		if err != nil {
			log.Fatal(err)
		}
	}
	lsm.LevelSizes[currentLevel-1] -= numOfCompacted
}

// Funkcija koja generise foldere do max nivoa
func (lsm *Lsm) GenerateLevelFolders() {
	path, err := filepath.Abs("files/sstable")
	if err != nil {
		log.Fatal(err)
	}

	for i := uint32(1); i < lsm.MaxLevel+1; i++ {
		_, err := os.Stat(path + "/level" + strconv.FormatUint(uint64(i), 10))
		if os.IsNotExist(err) {
			err = os.Mkdir(path+"/level"+strconv.FormatUint(uint64(i), 10), os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// Poziva se u mainu i pokrece izabranu kompakciju
func RunCompact() {
	lsm := ReadLsm()
	config := GetConfig()

	//Iteriramo po levelima
	//Preskacemo poslednji level jer se tu ne radi kompakcija
	if config.CompactionType == "size_tiered" {
		for currentLevel := uint32(1); currentLevel < lsm.MaxLevel; currentLevel++ {
			//Ukoliko ima bar 2 elementa u nivou pokrecemo
			if lsm.LevelSizes[currentLevel-1] >= 2 {
				lsm.SizeTieredCompaction(currentLevel)
			}
		}
	} else if config.CompactionType == "leveled" {
		for currentLevel := uint32(1); currentLevel < lsm.MaxLevel; currentLevel++ {
			//Ukoliko ima bar 1 element u nivou pokrecemo
			if lsm.LevelSizes[currentLevel-1] >= 1 {
				lsm.LeveledCompaction(currentLevel)
			}
		}
	}
}

// Size_tiered komapkcije
// spaja po 2 sstabele i prebacuje u naredni nivo
// ovo radi lancano do poslednjeg nivoa
func (lsm *Lsm) SizeTieredCompaction(currentLevel uint32) {
	size := getSSTableSize(currentLevel)

	//Uzimamo po 2 sstabele i radimo kompakciju nad njima
	for index := uint32(1); index < lsm.LevelSizes[currentLevel-1]; index += 2 {

		firstSStable := NewSSTable(size, lsm.GenerateSSTableName(currentLevel, index))
		secondSStable := NewSSTable(size, lsm.GenerateSSTableName(currentLevel, index+1))

		mergedKeys, mergedData := Merge2SSTables(firstSStable, secondSStable)

		lsm.LevelSizes[currentLevel]++
		mergedSSTable := NewSSTable(size*2, lsm.GenerateSSTableName(currentLevel+1, lsm.LevelSizes[currentLevel]))
		mergedSSTable.Flush(mergedKeys, mergedData)

		//Brisemo stare sstabele
		deleteSSTable(lsm.GenerateSSTableName(currentLevel, index))
		deleteSSTable(lsm.GenerateSSTableName(currentLevel, index+1))

	}
	lsm.RenameLevelSizeTiered(currentLevel) //Preimenujemo fajlove u trenutnom nivou
	lsm.Write()
}

// Leveled kompakcija
// Iz prvog nivoa sve podizemo na visi nivo.
// Svaki naredni put proveravamo koliko njih treba da podignemo kako bi uslov za taj nivo bio ispunjen.
// Ukoliko ima preklapanja sa narednim nivoom svi zajedno tabele se spajaju i ubacuju na odgovarajuce mesto.
// Ukoliko nema preklapanja trazimo gde treba da se ubace nove tabele i tu ih smestamo.
func (lsm *Lsm) LeveledCompaction(currentLevel uint32) {
	config := GetConfig()
	//Racuna broj sstabela koji je dozvoljen u trenutnom nivou
	maxSSTables := uint32(0) //U prvoj ne sme da ostane nijedna sstabela
	if currentLevel > 1 {
		maxSSTables = uint32(math.Pow(float64(config.LeveledCompactionMultiplier), float64(currentLevel)-1))
	}

	//Proveravamo da li je uopste potrebno raditi kompakciju na ovom nivou
	if lsm.LevelSizes[currentLevel-1] > maxSSTables {
		sstableArr := make([]SST, 0) //Niz sstabela koje ce se spajati
		if currentLevel == 1 {

			//Citamo prvog zbog minimalne i maksimalne vrednosti
			firstSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, 1))
			sstableArr = append(sstableArr, firstSSTable)
			minKey, maxKey := firstSSTable.GetRange()

			//Prolazimo kroz ceo prvi nivo (bez prve tabele jer je vec procitana)
			for index := uint32(2); index <= lsm.LevelSizes[currentLevel-1]; index++ {

				currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, index))

				//Obelezimo sve sstabele iz prvog nivoa
				sstableArr = append(sstableArr, currentSSTable)

				//Proveravamo range
				min, max := currentSSTable.GetRange()
				if min < minKey {
					minKey = min
				}
				if max > maxKey {
					maxKey = max
				}
			}

			//Cuva indekse od izabranih tabela iz narednog nivoa koje ulaze u kompakciju
			chosenIndexes := make([]uint32, 0)

			//Prolazimo kroz naredni nivo
			for index := uint32(1); index <= lsm.LevelSizes[currentLevel]; index++ {
				currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel+1, index))

				//Biramo samo one koji upadaju u opseg
				firstKey, lastKey := currentSSTable.GetRange()
				if !(lastKey < minKey || firstKey > maxKey) {
					sstableArr = append(sstableArr, currentSSTable)
					chosenIndexes = append(chosenIndexes, index)
				}

			}

			//MERGE
			numOfCreatedFiles := lsm.MergeSSTables(sstableArr, currentLevel)

			//Brisemo sve fajlove iz prvog nivoa
			for i := uint32(1); i <= lsm.LevelSizes[currentLevel-1]; i++ {
				deleteSSTable(lsm.GenerateSSTableName(currentLevel, i))
			}

			//Brisemo sve izabrane fajlove iz drugog dela
			for i := 0; i < len(chosenIndexes); i++ {
				deleteSSTable(lsm.GenerateSSTableName(currentLevel+1, chosenIndexes[i]))
			}

			//Rename fajlova
			lsm.RenameLevelLeveled(currentLevel+1, numOfCreatedFiles, chosenIndexes)
			lsm.LevelSizes[0] = 0 //Posto smo sve prebacili u naredni nivo
			lsm.Write()

		} else {
			sstablesToCompactNum := lsm.LevelSizes[currentLevel-1] - maxSSTables

			//Citamo prvog zbog minimalne i maksimalne vrednosti
			firstSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, 1))
			sstableArr = append(sstableArr, firstSSTable)
			minKey, maxKey := firstSSTable.GetRange()

			//Prolazimo kroz trenutan nivo (bez prve tabele jer je vec procitana)
			//dodajemo samo toliko tabela koliko je potrebno da bi isli ispod ogranicenja nivoa
			for index := uint32(2); index <= sstablesToCompactNum; index++ {

				currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, index))

				//Obelezimo sve sstabele iz prvog nivoa
				sstableArr = append(sstableArr, currentSSTable)

				//Proveravamo range
				min, max := currentSSTable.GetRange()
				if min < minKey {
					minKey = min
				}
				if max > maxKey {
					maxKey = max
				}
			}

			//Cuva indekse od izabranih tabela iz narednog nivoa koje ulaze u kompakciju
			chosenIndexes := make([]uint32, 0)

			//Prolazimo kroz naredni nivo
			for index := uint32(1); index <= lsm.LevelSizes[currentLevel]; index++ {
				currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel+1, index))

				//Biramo samo one koji upadaju u opseg
				firstKey, lastKey := currentSSTable.GetRange()
				if !(lastKey < minKey || firstKey > maxKey) {
					sstableArr = append(sstableArr, currentSSTable)
					chosenIndexes = append(chosenIndexes, index)
				}

			}

			//MERGE
			numOfCreatedFiles := lsm.MergeSSTables(sstableArr, currentLevel)

			//Brisemo odabrane fajlove iz prvog nivoa
			for i := uint32(1); i <= sstablesToCompactNum; i++ {
				deleteSSTable(lsm.GenerateSSTableName(currentLevel, i))
			}

			//Brisemo sve izabrane fajlove iz drugog dela
			for i := 0; i < len(chosenIndexes); i++ {
				deleteSSTable(lsm.GenerateSSTableName(currentLevel+1, chosenIndexes[i]))
			}

			//Rename fajlova
			lsm.RenameLevelLeveled(currentLevel+1, numOfCreatedFiles, chosenIndexes)
			lsm.UpdateCurrentLevelNames(currentLevel, sstablesToCompactNum) //Menja imena od preostalih fajlova u trenutnom nivou
			lsm.Write()
		}
	}
}

// Spaja 2 sstabele
func Merge2SSTables(firstSStable SST, secondSStable SST) ([]string, []*Data) {
	file1, data1End := firstSStable.GoToData()
	file2, data2End := secondSStable.GoToData()

	mergedKeys := make([]string, 0)
	mergedData := make([]*Data, 0)

	key1, data1 := "", new(Data)
	key2, data2 := "", new(Data)

	//Pomocne promenljive koje nam govore da li treba ici na sledeci element
	toRead1 := true
	toRead2 := true
	for true {
		//Kraj prve tabele
		if isEndOfData(file1, data1End) {
			if isEndOfData(file2, data2End) {
				break
			}

			if key1 == key2 {
				key2, data2 = ByteToData(file2)
			}

			//Prolazimo samo kroz drugu tabelu da prebacimo ostatak
			for true {
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
				if isEndOfData(file2, data2End) {
					break
				}
				key2, data2 = ByteToData(file2)
			}
			break
		}

		//Kraj druge tabele
		if isEndOfData(file2, data2End) {
			//Ukoliko su bili jednaki moramo preskociti trenutan
			if key1 == key2 {
				key1, data1 = ByteToData(file1)
			}

			//Prolazimo samo kroz prvu tabelu da prebacimo ostatak
			for true {
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
				if isEndOfData(file1, data1End) {
					break
				}
				key1, data1 = ByteToData(file1)
			}
			break

		}

		if toRead1 {
			key1, data1 = ByteToData(file1)
		}
		if toRead2 {
			key2, data2 = ByteToData(file2)
		}

		if key1 == key2 {
			if data2.Timestamp >= data1.Timestamp {
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
			} else {
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
			}
			toRead1 = true
			toRead2 = true
		} else if key1 < key2 {
			mergedKeys = append(mergedKeys, key1)
			mergedData = append(mergedData, data1)
			toRead1 = true
			toRead2 = false
		} else if key2 < key1 {
			mergedKeys = append(mergedKeys, key2)
			mergedData = append(mergedData, data2)
			toRead1 = false
			toRead2 = true
		}

	}
	err := file1.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = file2.Close()
	if err != nil {
		log.Fatal(err)
	}
	return mergedKeys, mergedData
}

// Vraca broj koliko je kreirano novih sstabela u narednom nivou
func (lsm *Lsm) MergeSSTables(sstables []SST, currentLevel uint32) uint32 {
	config := GetConfig()
	numOfCreatedFiles := uint32(0)

	files := make([]*os.File, 0)    //Ovde cuvamo otvorene fajlove od svih sstabela
	dataEnds := make([]uint64, 0)   //Ovde cuvamo krajeve data zona za svaku sstabelu
	isEndOfFiles := make([]bool, 0) //Ovde cuvamo bool vrednost da li je cela sstabela predjena

	keys := make([]string, len(sstables)) //Ovde cuvamo trenutne kljuceve
	data := make([]*Data, len(sstables))  //Ovde cuvamo trenutan podatak
	toRead := make([]bool, 0)             //Flag da li je potrebno citanje sledeceg elementa

	//Identifikacija sstabela i dodavanje u niz
	for i := 0; i < len(sstables); i++ {
		currentSSTable := sstables[i]

		//otvaramo fajl i pozicioniramo se na data zonu
		file, dataEnd := currentSSTable.GoToData()
		files = append(files, file)
		dataEnds = append(dataEnds, dataEnd)

		isEndOfFiles = append(isEndOfFiles, false) //na pocetku inicijalizujemo na false

		toRead = append(toRead, true) //Uvek prve elemente citamo
	}

	mergedKeys := make([]string, 0)
	mergedData := make([]*Data, 0)

	for true {
		tempKeys := make([]string, 0) //Cuvamo kljuceve koje uporedjujemo (necemo cuvati kljuceve od fajlova koji su predjeni)
		tempData := make([]*Data, 0)  //Cuvamo podatke koje uporedjujemo

		//Prolazimo kroz sve fajlove
		for i := 0; i < len(files); i++ {
			if !isEndOfData(files[i], dataEnds[i]) {
				if toRead[i] {
					keys[i], data[i] = ByteToData(files[i])
				}
				tempKeys = append(tempKeys, keys[i])
				tempData = append(tempData, data[i])
			} else {
				isEndOfFiles[i] = true
			}
		}

		//Svi su ubaceni i kraj
		if len(tempKeys) < 1 {
			break
		}

		//Trazimo koji je element sa najmanjom vrednoscu
		minKey := tempKeys[0]
		for i := 1; i < len(tempKeys); i++ {
			if tempKeys[i] < minKey {
				minKey = tempKeys[i]
			}
		}

		//Trazimo koji element treba da procitamo po najvecem timestampu
		newestData := new(Data)
		newestData.Timestamp = 0
		for i := 0; i < len(tempData); i++ {
			if tempKeys[i] == minKey {
				if tempData[i].Timestamp > newestData.Timestamp {
					newestData = tempData[i]
				}
			}
		}

		//Obelezavamo sve koji su jednaki izabranom kljucu da se citaju
		//Ostali zadrzavaju vrednost
		for i := 0; i < len(toRead); i++ {
			toRead[i] = keys[i] == minKey //Ovo neces videti u Novom Sadu :)
		}

		//Dodajemo u red za upis u novu sstabelu
		mergedKeys = append(mergedKeys, minKey)
		mergedData = append(mergedData, newestData)

		//Proveravamo da li smo napunili sstabelu
		//Ukoliko jesmo flushujemo u visi nivo
		if len(mergedKeys) >= int(config.MemtableSize) {
			lsm.LevelSizes[currentLevel]++ //Povecavamo broj fajlova u visem nivou
			mergedSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel+1, lsm.LevelSizes[currentLevel]))
			mergedSSTable.Flush(mergedKeys, mergedData)

			//Resetujemo nizove
			mergedKeys = make([]string, 0)
			mergedData = make([]*Data, 0)

			//Povecavamo counter
			numOfCreatedFiles++
		}
	}

	//Ukoliko se nije flush sam izazvao a ima jos fajlova moramo ih zapisati
	if len(mergedKeys) > 0 {
		lsm.LevelSizes[currentLevel]++ //Povecavamo broj fajlova u visem nivou
		mergedSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel+1, lsm.LevelSizes[currentLevel]))
		mergedSSTable.Flush(mergedKeys, mergedData)

		//Povecavamo counter
		numOfCreatedFiles++
	}

	//Zatvaramo sve fajlove
	for _, file := range files {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	return numOfCreatedFiles
}

// Proveravamo da li smo prosli data zonu
func isEndOfData(file *os.File, dataEnd uint64) bool {
	currentOffset, err := file.Seek(0, 1)
	if err != nil {
		log.Fatal(err)
	}
	if uint64(currentOffset) >= dataEnd {
		return true
	}
	return false
}

// Brise sstabelu
func deleteSSTable(directory string) {
	err := os.RemoveAll("files/sstable/" + directory)
	if err != nil {
		log.Fatal(err)
	}
}

// Racuna velicinu sstabele za zadati nivo
func getSSTableSize(currentLevel uint32) uint32 {
	config := GetConfig()
	//Racunamo velicinu naredne sstabele kao duplu od prethodne
	//jer ne znamo kolika ce tacno biti velicina,
	//mada ona nije ni toliko bitna jer je koristi samo bloomfilter za inicijalizaciju
	//On prima ocekivani broj elemenata tako da ovo nece biti greska
	return uint32(math.Pow(2, float64(currentLevel-1)) * float64(config.MemtableSize))
}

// Trazi kljuc unutar svih sstabela
func (lsm *Lsm) Find(key string) (bool, *Data) {
	config := GetConfig()
	//iteriramo po nivoima
	for currentLevel := uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++ {
		size := getSSTableSize(currentLevel)
		//iteriramo po sstabelama kako su dodavane(od najveceg indeksa, noviji ce se prvi citati)
		if currentLevel == 1 || config.CompactionType == "size_tiered" {
			for i := lsm.LevelSizes[currentLevel-1]; i > 0; i-- {
				currentSSTable := NewSSTable(size, lsm.GenerateSSTableName(currentLevel, i))
				found, data := currentSSTable.Find(key)
				if found {
					return found, data
				}
			}
		} else { //Ukoliko je leveled kompakcija u visim nivoima svi podaci ce biti sortirani tako da treba citati sstabele redom
			for i := uint32(1); i <= lsm.LevelSizes[currentLevel-1]; i++ {
				currentSSTable := NewSSTable(size, lsm.GenerateSSTableName(currentLevel, i))
				found, data := currentSSTable.Find(key)
				if found {
					return found, data
				}
			}
		}
	}
	return false, nil

}

// ---------- SKENIRANJE VISE PODATAKA ----------

// iterira po svim sstabelama i prekida ako je napunio trazenu stranicu
func (lsm *Lsm) RangeScan(minKey string, maxKey string, scan *Scan) {
	config := GetConfig()

	//iteriramo po nivoima
	for currentLevel := uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++ {
		size := getSSTableSize(currentLevel)
		//iteriramo po sstabelama kako su dodavane(od najveceg indeksa, noviji ce se prvi citati)
		if currentLevel == 1 || config.CompactionType == "size_tiered" {
			for i := lsm.LevelSizes[currentLevel-1]; i > 0; i-- {
				currentSSTable := NewSSTable(size, lsm.GenerateSSTableName(currentLevel, i))
				currentSSTable.RangeScan(minKey, maxKey, scan)
				if scan.FoundResults >= scan.SelectedPageEnd {
					return
				}
			}
		} else { //Ukoliko je leveled kompakcija u visim nivoima svi podaci ce biti sortirani tako da treba citati sstabele redom
			for i := uint32(1); i <= lsm.LevelSizes[currentLevel-1]; i++ {
				currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, i))
				currentSSTable.RangeScan(minKey, maxKey, scan)
				if scan.FoundResults >= scan.SelectedPageEnd {
					return
				}
			}
		}
	}
}

// iterira po svim sstabelama i prekida ako je napunio trazenu stranicu
func (lsm *Lsm) ListScan(prefix string, scan *Scan) {
	config := GetConfig()
	//iteriramo po nivoima
	for currentLevel := uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++ {
		size := getSSTableSize(currentLevel)
		//iteriramo po sstabelama kako su dodavane(od najveceg indeksa, noviji ce se prvi citati)
		if currentLevel == 1 || config.CompactionType == "size_tiered" {
			for i := lsm.LevelSizes[currentLevel-1]; i > 0; i-- {
				currentSSTable := NewSSTable(size, lsm.GenerateSSTableName(currentLevel, i))
				currentSSTable.ListScan(prefix, scan)
				if scan.FoundResults >= scan.SelectedPageEnd {
					return
				}
			}
		} else { //Ukoliko je leveled kompakcija u visim nivoima svi podaci ce biti sortirani tako da treba citati sstabele redom
			for i := uint32(1); i <= lsm.LevelSizes[currentLevel-1]; i++ {
				currentSSTable := NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, i))
				currentSSTable.ListScan(prefix, scan)
				if scan.FoundResults >= scan.SelectedPageEnd {
					return
				}
			}
		}
	}
}

// ---------- PRINT IZ MEMORIJE -----------

func (lsm *Lsm) Print() {
	config := GetConfig()
	for currentLevel := uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++ {
		if lsm.LevelSizes[currentLevel-1] > 0 {
			fmt.Println("--------------------- LEVEL ", currentLevel, " ---------------------")
			for i := uint32(1); i <= lsm.LevelSizes[currentLevel-1]; i++ {
				fmt.Println("--------------------- SSTABLE - ", i, " ---------------------")
				NewSSTable(uint32(config.MemtableSize), lsm.GenerateSSTableName(currentLevel, i)).ReadData()
				time.Sleep(time.Millisecond * 10) //Da ne bi preforsirali sistem u printovanju, u suprotnom se moze desiti da zabode
			}
		}
	}
}
