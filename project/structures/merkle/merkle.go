package merkle

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

type Node struct {
	Data  []byte
	hash  [20]byte
	left  *Node
	right *Node
}

type MerkleRoot struct {
	Root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.Root.String()
}

// Ucitavanje ulaznih podataka u listu nodova
func ReadFile(fileName string) []*Node {
	nodes := make([]*Node, 0)
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		string_data := scanner.Text()

		node := Node{Data: []byte(string_data)}
		nodes = append(nodes, &node)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
	return nodes
}

//Upisivanje merkle root-a u metadata file
func WriteFile(file *os.File, rootNode *Node) {
	writer := bufio.NewWriter(file)

	_, err := writer.WriteString(rootNode.String())
	if err != nil {
		fmt.Println(err)
		return
	}

	err = writer.Flush()
	if err != nil {
		log.Fatal(err)
	}
}

// Prolazi kroz nodove koristi pomocnu listu
// smanjuje je i kada dostigne velicinu 1 vraca Root node
func MakeMerkel(nodes []*Node) *MerkleRoot {
	for len(nodes) > 1 {
		newNodes := make([]*Node, 0)
		for i := 0; i < len(nodes); i += 2 {
			n := new(Node)
			n.left = nodes[i]
			if i+1 > len(nodes)-1 {
				n.right = &Node{hash: Hash([]byte{})}
			} else {
				n.right = nodes[i+1]
			}
			Data := make([]byte, 0)
			Data = append(Data, n.left.Data...)
			Data = append(Data, n.right.Data...)
			n.hash = Hash(Data)
			newNodes = append(newNodes, n)
		}
		nodes = newNodes
	}
	Root := &MerkleRoot{Root: nodes[0]}
	return Root
}

func (n *Node) String() string {
	return hex.EncodeToString(n.hash[:])
}

func (n *Node) Data_String() string {
	return string(n.Data[:])
}

func Hash(Data []byte) [20]byte {
	return sha1.Sum(Data)
}