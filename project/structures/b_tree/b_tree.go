package b_tree

import (
	"fmt"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/scan"
	"strings"
)

type BTreeNode struct {
	keys     []string //Kljucevi
	Values   map[string]*Data
	children []*BTreeNode //Pokazivaci na decu
	parent   *BTreeNode
}

type BTree struct {
	Root    *BTreeNode
	m       uint //Red stabla(maks broj dece)
	maxKeys uint //Maksimalan broj kljuceva
	Size    uint //Broj elemenata u stablu
}

func NewBTreeNode(parent *BTreeNode) *BTreeNode {
	bTreeNode := new(BTreeNode)
	bTreeNode.keys = make([]string, 0)
	bTreeNode.Values = make(map[string]*Data)
	bTreeNode.children = make([]*BTreeNode, 0)
	bTreeNode.parent = parent
	return bTreeNode
}

// m = maksimalan broj dece
func NewBTree(m uint) *BTree {
	bTree := new(BTree)
	bTree.Root = nil
	bTree.m = m
	bTree.maxKeys = m - 1
	bTree.Size = 0
	return bTree
}

// Nas bubblesort koji sortira uint
func BubbleSort(keys []string) []string {
	for i := 0; i < len(keys)-1; i++ {
		for j := 0; j < len(keys)-i-1; j++ {
			if keys[j] > keys[j+1] {
				keys[j], keys[j+1] = keys[j+1], keys[j]
			}
		}
	}
	return keys
}

// Brise element liste na datom indeksu
func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

// Trazi cvor sa kljucem
func (bTree *BTree) FindNode(keyToFind string) (bool, *BTreeNode) {
	//Da ne puca ako je prazan koren
	if bTree.Root == nil {
		return false, nil
	}

	currentNode := bTree.Root //Pocinjemo od korena
	for true {
		numberOfKeys := len(currentNode.keys) //Broj kljuceva za pretragu

		//Iteriramo po kljucevima
		for index, key := range currentNode.keys {
			if key == keyToFind {
				return true, currentNode
			} else if keyToFind < key {
				if len(currentNode.children) == 0 {
					return false, currentNode //vracamo roditelja
				}
				currentNode = currentNode.children[index]
				break
			} else if keyToFind > key && index == numberOfKeys-1 {
				if len(currentNode.children) == 0 {
					return false, currentNode //vracamo roditelja
				}
				currentNode = currentNode.children[index+1]
				break
			} else if keyToFind > key && index != numberOfKeys-1 {
				continue
			}

		}
	}
	return false, currentNode
}

// Deli cvor rekurzivno do korena po potrebi
func (bTree *BTree) splitNode(node *BTreeNode) {
	parent := node.parent

	//Uslov za izlaz iz rekurzije, ako je dosao do korena
	if parent == nil {
		newRoot := NewBTreeNode(nil)
		bTree.Root = newRoot
		parent = newRoot
	}

	//Ukoliko pre nije imao nijedno dete
	if len(parent.children) == 0 {
		parent.children = append(parent.children, nil) //zauzimamo jedno mesto za desno dete
	}
	parent.children = append(parent.children, nil) //zauzimamo jedno mesto za levo dete

	//Delimo cvor na tri dela
	middleIndex := bTree.maxKeys / 2
	middleKey := node.keys[middleIndex]
	// middleVal := node.Values[middleIndex]

	// ----- left -----
	leftNode := NewBTreeNode(parent)
	//dodajem kljuceve i vrednosti
	for _, key := range node.keys[0:middleIndex] {
		leftNode.keys = append(leftNode.keys, key)
		leftNode.Values[key] = node.Values[key]
	}
	//dodajem decu, ukoliko je ima
	if len(node.children) != 0 {
		for _, child := range node.children[0 : middleIndex+1] {
			leftNode.children = append(leftNode.children, child)
			child.parent = leftNode
		}
	}

	// ----- right -----
	rightNode := NewBTreeNode(parent)
	//dodajem kljuceve i vrednosti
	for _, key := range node.keys[middleIndex+1:] {
		rightNode.keys = append(rightNode.keys, key)
		rightNode.Values[key] = node.Values[key]
	}
	//dodajem decu, ukoliko je ima
	if len(node.children) != 0 {
		for _, child := range node.children[middleIndex+1:] {
			rightNode.children = append(rightNode.children, child)
			child.parent = rightNode
		}
	}

	// ----- Ubacujem srednji element u roditelja -----
	parent.keys = append(parent.keys, middleKey)
	parent.Values[middleKey] = node.Values[middleKey]
	parent.keys = BubbleSort(parent.keys)

	// //Brisem stari cvor
	// delete(node.Values, middleKey) //Brisemo value iz naseg cvora
	// node.keys = RemoveIndex(node.keys, int(middleIndex))
	// node.keys = BubbleSort(node.keys)

	addedKeyIndex := 0
	for index, k := range parent.keys {
		if k == middleKey {
			addedKeyIndex = index
			break
		}
	}
	//Pomeramo svu decu u desno
	// parent.children = append(parent.children, nil) //zauzimamo mesto

	for i := len(parent.children) - 2; i > addedKeyIndex; i-- {
		parent.children[i+1] = parent.children[i]
	}
	//Dodajemo podeljene node-ove kao decu
	parent.children[addedKeyIndex] = leftNode
	parent.children[addedKeyIndex+1] = rightNode

	//Proveravamo da li roditelj treba da se deli - REKURZIJA
	if len(parent.keys) > int(bTree.maxKeys) {
		bTree.splitNode(parent)
	}
}

// Treci parametar govori da li se rotira sa desnim rodjakom
// false znaci da se rotira sa levim
func (bTree *BTree) rotateNodes(node *BTreeNode, sibling *BTreeNode, isRight bool) {
	if isRight {
		//Najveceg iz naseg cvora dizemo
		keyFromNode := node.keys[len(node.keys)-1]

		keyFromParent := ""
		indexFromParent := 0
		//Trazim kojeg iz roditeljskog spustam dole(prvog veceg)
		for index, key := range node.parent.keys {
			if key > keyFromNode {
				keyFromParent = key
				indexFromParent = int(index)
				break
			}
		}

		node.parent.keys[indexFromParent] = keyFromNode //Prepisujemo preko starog
		node.parent.Values[keyFromNode] = node.Values[keyFromNode]
		delete(node.Values, keyFromNode) //Brisemo value iz naseg cvora
		node.keys = RemoveIndex(node.keys, len(node.keys)-1)
		node.parent.keys = BubbleSort(node.parent.keys)

		//Prvog najveceg iz roditelja spustamo
		sibling.keys = append(sibling.keys, keyFromParent)
		sibling.Values[keyFromParent] = node.parent.Values[keyFromParent]
		delete(node.parent.Values, keyFromParent) //Brisemo value iz roditelja
		sibling.keys = BubbleSort(sibling.keys)

	} else {
		//Najmanjeg iz naseg cvora dizemo
		keyFromNode := node.keys[0]

		keyFromParent := ""
		indexFromParent := 0
		//Trazim kojeg iz roditeljskog spustam dole(prvog manjeg)
		for index := len(node.parent.keys) - 1; index >= 0; index-- {
			if node.parent.keys[index] < keyFromNode {
				keyFromParent = node.parent.keys[index]
				indexFromParent = int(index)
				break
			}
		}

		node.parent.keys[indexFromParent] = keyFromNode //Prepisujemo preko starog
		node.parent.Values[keyFromNode] = node.Values[keyFromNode]
		delete(node.Values, keyFromNode) //Brisemo value iz naseg cvora
		node.keys = RemoveIndex(node.keys, 0)
		node.parent.keys = BubbleSort(node.parent.keys)

		//Prvog najmanjeg iz roditelja spustamo
		sibling.keys = append(sibling.keys, keyFromParent)
		sibling.Values[keyFromParent] = node.parent.Values[keyFromParent]
		delete(node.parent.Values, keyFromParent) //Brisemo value iz roditelja
		sibling.keys = BubbleSort(sibling.keys)
	}
}

// Ubacuje kljuc
func (bTree *BTree) Put(key string, data *Data) {
	//U slucaju da koren ne postoji
	if bTree.Root == nil {
		bTree.Root = NewBTreeNode(nil) //Nema roditelja :(
		bTree.Root.keys = append(bTree.Root.keys, key)
		bTree.Root.keys = BubbleSort(bTree.Root.keys)
		bTree.Root.Values[key] = data
		bTree.Size++
		return
	}

	found, node := bTree.FindNode(key)

	//Ukoliko vec postoji samo izmenimo
	if found {
		node.Values[key] = data
		return
	}

	//Dodamo element
	node.keys = append(node.keys, key)
	node.Values[key] = data
	node.keys = BubbleSort(node.keys)
	bTree.Size++

	//Ukoliko nema mesta u trenutnom cvoru
	if len(node.keys) > int(bTree.maxKeys) {
		if node.parent != nil {
			siblings := node.parent.children
			for index, child := range siblings {
				//Trazim indeks trenutnog node-a
				if child == node {
					//Proveravam da li ima levog/desnog suseda
					if len(siblings) == 1 {
						break
					} else if index == 0 {
						rightSibling := siblings[index+1]
						if len(rightSibling.keys) == int(bTree.maxKeys) {
							break
						}
						//Rotacija...
						bTree.rotateNodes(node, rightSibling, true)
						return
					} else if index == len(siblings)-1 {
						leftSibling := siblings[index-1]
						if len(leftSibling.keys) == int(bTree.maxKeys) {
							break
						}
						//Rotacija...
						bTree.rotateNodes(node, leftSibling, false)
						return
					} else {
						leftSibling := siblings[index-1]
						if len(leftSibling.keys) < int(bTree.maxKeys) {
							//Rotacija
							bTree.rotateNodes(node, leftSibling, false)
							return
						}
						rightSibling := siblings[index+1]
						if len(rightSibling.keys) < int(bTree.maxKeys) {
							//Rotacija
							bTree.rotateNodes(node, rightSibling, true)
							return
						}
						break
					}
				}
			}
		}

		//Ukoliko ne moze da rotira onda splituje
		bTree.splitNode(node)
	}
}

// Logicko brisanje - postavlja tombstone na true
func (bTree *BTree) Remove(key string) bool{
	found, node := bTree.FindNode(key)
	if !found {
		return false
	}
	node.Values[key].Tombstone = true
	return true
}

// INORDER obilazak
func (bTree *BTree) InorderTraverse(node *BTreeNode, keys *[]string, Values *[]*Data) {
	if node == nil {
		return
	}
	for i := 0; i < len(node.children)-1; i++ {
		bTree.InorderTraverse(node.children[i], keys, Values)
		if i < len(node.keys) {
			*keys = append(*keys, node.keys[i])
			*Values = append(*Values, node.Values[node.keys[i]])
		}
	}
	if len(node.children) > 0 {
		bTree.InorderTraverse(node.children[len(node.children)-1], keys, Values)
	} else {
		for i := 0; i < len(node.keys); i++ {
			*keys = append(*keys, node.keys[i])
			*Values = append(*Values, node.Values[node.keys[i]])
		}
	}
}

func (bTree *BTree) RangeScan(minKey string, maxKey string,node *BTreeNode, scan *Scan) {
	if node == nil {
		return
	}
	if scan.FoundResults >= scan.SelectedPageEnd{
		return
	}
	for i := 0; i < len(node.children)-1; i++ {
		bTree.RangeScan(minKey, maxKey, node.children[i], scan)
		if i < len(node.keys) {
			if node.keys[i] >= minKey && node.keys[i] <= maxKey{
				if !node.Values[node.keys[i]].Tombstone{
					//Obelezimo dati kljuc da je procitan
					scan.SelectedKeys[node.keys[i]] = true

					scan.FoundResults++
					if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd{
						scan.Keys = append(scan.Keys, node.keys[i])
						scan.Data = append(scan.Data, node.Values[node.keys[i]])
					}
				} else {
					//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
					scan.RemovedKeys[node.keys[i]] = true
				}
			}
		}
	}
	if len(node.children) > 0 {
		bTree.RangeScan(minKey, maxKey, node.children[len(node.children)-1], scan)
	} else {
		for i := 0; i < len(node.keys); i++ {
			if node.keys[i] >= minKey && node.keys[i] <= maxKey{
				if !node.Values[node.keys[i]].Tombstone{
					//Obelezimo dati kljuc da je procitan
					scan.SelectedKeys[node.keys[i]] = true

					scan.FoundResults++
					if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd{
						scan.Keys = append(scan.Keys, node.keys[i])
						scan.Data = append(scan.Data, node.Values[node.keys[i]])
					}
				} else {
					//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
					scan.RemovedKeys[node.keys[i]] = true
				}
			}
		}
	}
}

func (bTree *BTree) ListScan(prefix string,node *BTreeNode, scan *Scan) {
	if node == nil {
		return
	}
	if scan.FoundResults >= scan.SelectedPageEnd{
		return
	}
	for i := 0; i < len(node.children)-1; i++ {
		bTree.ListScan(prefix, node.children[i], scan)
		if i < len(node.keys) {
			if strings.HasPrefix(node.keys[i], prefix){
				if !node.Values[node.keys[i]].Tombstone{
					//Obelezimo dati kljuc da je procitan
					scan.SelectedKeys[node.keys[i]] = true

					scan.FoundResults++
					if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd{
						scan.Keys = append(scan.Keys, node.keys[i])
						scan.Data = append(scan.Data, node.Values[node.keys[i]])
					}
				} else {
					//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
					scan.RemovedKeys[node.keys[i]] = true
				}
			}
		}
	}
	if len(node.children) > 0 {
		bTree.ListScan(prefix, node.children[len(node.children)-1], scan)
	} else {
		for i := 0; i < len(node.keys); i++ {
			if strings.HasPrefix(node.keys[i], prefix){
				if !node.Values[node.keys[i]].Tombstone{
					//Obelezimo dati kljuc da je procitan
					scan.SelectedKeys[node.keys[i]] = true

					scan.FoundResults++
					if scan.FoundResults >= scan.SelectedPageStart && scan.FoundResults <= scan.SelectedPageEnd{
						scan.Keys = append(scan.Keys, node.keys[i])
						scan.Data = append(scan.Data, node.Values[node.keys[i]])
					}
				} else {
					//Posto je obrisan oznacicemo ga kao obrisanog da se ne uzima u obzir dalje
					scan.RemovedKeys[node.keys[i]] = true
				}
			}
		}
	}
}

// Ispis b stabla
func (t *BTree) PrintBTree() {
	var queue []*BTreeNode
	queue = append(queue, t.Root)
	level := 0

	for len(queue) > 0 {
		fmt.Println("----------------------------------------")
		fmt.Println("Level: ", level)
		fmt.Println("----------------------------------------")

		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[i]
			fmt.Print(strings.Repeat("  ", level))
			fmt.Print("Keys: ")
			for _, key := range current.keys {
				if current.Values[key].Tombstone {
					fmt.Print("(", key, ")", " ")
				} else {
					fmt.Print(key, " ")
				}
			}
			fmt.Print(" | Children: ")
			for _, child := range current.children {
				queue = append(queue, child)
				fmt.Print(child.keys, " ")
			}
			if current.parent != nil {
				fmt.Print(" | Parent: ", current.parent.keys)
			}
			fmt.Println()
		}
		level++
		queue = queue[levelSize:]
	}
}
