package skiplist

import (
	"fmt"
	"math"
	"math/rand"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/scan"
	"strings"
)

type SkipList struct {
	maxHeight uint
	height    uint
	size      uint
	head      *SkipListNode
}

type SkipListNode struct {
	//key uint da bi bilo isto kao kod B_tree
	key       string
	Data 	  *Data
	next      []*SkipListNode
}

func (s *SkipList) roll() uint {
	level := uint(1)
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			if level > s.height {
				s.height = level
			}
			return level
		}
	}
	if level > s.height {
		s.height = level
	}
	return level
}

//Konstruktor
func NewSkipList(maxh uint) *SkipList {
	skipList := new(SkipList)
	skipList.maxHeight = maxh
	skipList.height = 0
	skipList.size = 0
	skipList.head = &SkipListNode{
		key:   "",
		Data: new(Data),
		next:  make([]*SkipListNode, maxh),
	}
	return skipList
}

func (s *SkipList) GetSize() uint {
	return s.size
}

//Trazi zadati kljuc u skiplisti
func (s *SkipList) Find(key string) (*SkipListNode, bool) {
	currentNode := s.head
	for currentLevel := int(s.height) - 1; currentLevel >= 0; currentLevel-- {
		if currentNode.key == key {
			return currentNode, true
		} else if currentNode.key < key {
			for currentNode.key <= key {
				if currentNode.key == key {
					return currentNode, true
				}
				next := currentNode.next[currentLevel]
				if next == nil || next.key > key {
					break
				}
				currentNode = next
			}
		}
	}
	return currentNode, false
}

// Prevezuje sve pokazivace nakon ubacivanja
func (s *SkipList) updateNodePointers(node *SkipListNode, minHeight int) {
	currentNode := s.head
	nodeHeight := len(node.next)
	key := node.key
	for currentLevel := nodeHeight - 1; currentLevel > minHeight; currentLevel-- {
		if currentNode.key < key {
			for currentNode.key < key {
				next := currentNode.next[currentLevel]
				//Pre nego sto se spustamo nivo dole prevezemo pokazivace
				if next == nil || next.key > key {
					tempNextNode := next
					currentNode.next[currentLevel] = node
					node.next[currentLevel] = tempNextNode
					break
				}
				currentNode = next
			}
		}
	}
}

//Ubacuje kljuc i vrednost u skiplistu
func (s *SkipList) Put(key string, data *Data) {
	node, found := s.Find(key)
	//update ako ga je nasao
	if found {
		node.Data = data
	} else {
		//Pravimo nov node
		level := s.roll()
		newNode := &SkipListNode{
			key:       key,
			Data:      data,
			next:      make([]*SkipListNode, level),
		}
		s.size += 1

		//Prevezujemo pokazivace do visine pronadjenog node-a
		for currentLevel := int(math.Min(float64(len(node.next)), float64(level))) - 1; currentLevel >= 0; currentLevel-- {
			tempNextNode := node.next[currentLevel]
			node.next[currentLevel] = newNode
			newNode.next[currentLevel] = tempNextNode
		}
		//Prevezujemo preostale pokazivace
		//u slucaju da je visina naseg node-a veca od pronadjenog
		if uint(len(node.next)) < level {
			s.updateNodePointers(newNode, len(node.next)-1)
		}
	}
}

//Regulise visinu skipliste
func (s *SkipList) updateHeight() {
	for currentLevel := s.height - 1; currentLevel >= 0; currentLevel-- {
		if s.head.next[currentLevel] == nil {
			s.height--
		} else {
			break
		}
	}
}

// ovo je fizicko brisanje
// Ne koristimo nigde
func (s *SkipList) RemovePhysical(key string) {
	node, found := s.Find(key)
	currentNode := s.head
	if found {
		//Prevezujemo pokazivace do visine pronadjenog node-a
		for currentLevel := len(node.next) - 1; currentLevel >= 0; currentLevel-- {
			for currentNode != nil {
				if currentNode.next[currentLevel].key == key {
					//Prevezi
					currentNode.next[currentLevel] = currentNode.next[currentLevel].next[currentLevel]
					break
				}
				currentNode = currentNode.next[currentLevel]
			}
		}
	}
	s.updateHeight()
}

// ovo je logicno brisanje
func (s *SkipList) Remove(key string) bool {
	node, found := s.Find(key)
	currentNode := s.head
	if found {
		//Prevezujemo pokazivace do visine pronadjenog node-a
		for currentLevel := len(node.next) - 1; currentLevel >= 0; currentLevel-- {
			for currentNode != nil {
				if currentNode.next[currentLevel].key == key {
					//tombstone
					currentNode.next[currentLevel].Data.Tombstone = true
					break
				}
				currentNode = currentNode.next[currentLevel]
			}
		}	
	} 
	s.updateHeight()
	return found
}

// uzima sve podatke u sortiranom redosledu
func (s *SkipList) GetAllNodes(keys *[]string, values *[]*Data) {

	currentNode := s.head
	for currentNode.next[0] != nil {
		next := currentNode.next[0]
		data := next.Data

		*keys = append(*keys, next.key)
		*values = append(*values, data)

		currentNode = next
	}
}

//Ispis cele skipliste
func (s *SkipList) Print() {
	fmt.Println(strings.Repeat("_", 100))
	fmt.Println()
	currentNode := s.head.next[0]
	//level zero nodes
	nodeSlice := make([]*SkipListNode, 0)
	for currentNode != nil {
		nodeSlice = append(nodeSlice, currentNode)
		currentNode = currentNode.next[0]
	}

	for currentLevel := int(s.height) - 1; currentLevel >= 0; currentLevel-- {
		fmt.Print("head -")
		for i := 0; i < len(nodeSlice); i++ {
			if len(nodeSlice[i].next) > currentLevel {
				fmt.Print("> " + nodeSlice[i].key)
				fmt.Print(" -")
			} else {
				keyLen := len(nodeSlice[i].key)
				fmt.Print(strings.Repeat("-", keyLen+4))
			}
		}
		fmt.Print("> nil")
		fmt.Println()
	}
	fmt.Println(strings.Repeat("_", 100))
}


func (s *SkipList) RangeScan(minKey string, maxKey string, scan *Scan){
	currentNode := s.head
	for currentNode.next[0] != nil {
		if scan.FoundResults >= scan.SelectedPageEnd{
			return
		}

		next := currentNode.next[0]
		data := next.Data

		if next.key >= minKey && next.key <= maxKey{
			if !next.Data.Tombstone{
				//Obelezimo dati kljuc da je procitan
				scan.SelectedKeys[next.key] = true

				scan.FoundResults++
				if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd{
					scan.Keys = append(scan.Keys, next.key)
					scan.Data = append(scan.Data, data)
				}
			} else {
				//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
				scan.RemovedKeys[next.key] = true
			}
		} else if next.key > maxKey{
			return
		}


		currentNode = next
	}
}

func (s *SkipList) ListScan(prefix string, scan *Scan){
	currentNode := s.head
	for currentNode.next[0] != nil {
		if scan.FoundResults >= scan.SelectedPageEnd{
			return
		}

		next := currentNode.next[0]
		data := next.Data

		if strings.HasPrefix(next.key, prefix){
			if !next.Data.Tombstone{
				//Obelezimo dati kljuc da je procitan
				scan.SelectedKeys[next.key] = true

				scan.FoundResults++
				if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd{
					scan.Keys = append(scan.Keys, next.key)
					scan.Data = append(scan.Data, data)
				}
			} else {
				//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
				scan.RemovedKeys[next.key] = true
			}
		} else if next.key > prefix{
			return
		}

		currentNode = next
	}
}