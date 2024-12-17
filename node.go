package main

import (
	"crypto/rsa"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
)

var fingerTableLength = 6

// Chord ring uses a 2^6 space
// Identifiers for nodes and keys are of length 6
var chordBits = 6
var chordSpace = new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(chordBits)), nil) // 2^6 = 64

type fingerRecord struct {
	Id          []byte
	NodeAddress string
}

type ChordNode struct {
	Id          *big.Int
	Address     string
	Name        string
	FingerTable []fingerRecord
	NextFinger  int

	PredecessoAddress string
	SuccessorsAddress []string

	Bucket map[string]string // Store encrypted file data
	Backup map[string]string // Fault-tolerance

	PrivateEncryptionKey *rsa.PrivateKey
	PublicEncryptionKey  *rsa.PublicKey
	EncryptionEnabled    bool
}

type RPCStoreFileResponse struct {
	Successful bool
	Err        error
	Backup     bool
}

func (node *ChordNode) NewChord() {
	for i := 0; i < len(node.SuccessorsAddress); i++ {
		node.SuccessorsAddress[i] = node.Address
	}
}

func CreateNode(args Args) *ChordNode {
	node := &ChordNode{}

	nodeAddress := func() string {
		switch args.IPAddress {
		case "127.0.0.1", "localhost":
			return "127.0.0.1"
		case "0.0.0.0":
			return func() string {
				ip, err := getMachineIP()
				if err != nil {
					log.Println("Failed to get machine IP:", err)
					return getLocalAddress()
				}
				return ip
			}()
		default:
			return getLocalAddress()
		}
	}()

	node.Address = nodeAddress + ":" + strconv.Itoa(args.Port)

	//0-63
	node.Id = SHash(node.Address)
	node.Id.Mod(node.Id, chordSpace)

	node.FingerTable = make([]fingerRecord, fingerTableLength+1)

	node.NextFinger = 0

	node.PredecessoAddress = ""
	node.SuccessorsAddress = make([]string, args.R)

	//initiate id to n+2^(i-1), all addr to node.Addr
	node.initializeFingerTable()
	//initiate all to empty string
	node.initSuccessorsAddr()

	node.EncryptionEnabled = true

	node.Bucket = make(map[string]string)
	node.Backup = make(map[string]string)

	basePath := fmt.Sprintf("../files/N%s", node.Id.String())

	// Check if the directory exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
			log.Printf("Error creating directory %s: %v\n", basePath, err)
		} else {
			subDirs := []string{"upload", "chord_storage"}
			for _, subDir := range subDirs {
				dirPath := fmt.Sprintf("%s/%s", basePath, subDir)
				if _, err := os.Stat(dirPath); os.IsNotExist(err) {
					if mkdirErr := os.Mkdir(dirPath, os.ModePerm); mkdirErr != nil {
						log.Printf("Error creating subdirectory %s: %v\n", dirPath, mkdirErr)
					}
				}
			}
		}
	} else {
		log.Printf("Node directory already exists: %s\n", basePath)
	}

	// Always generate keys, even if directory exists
	node.generateRSAKeys(2048)

	return node

}

func (node *ChordNode) initializeFingerTable() {
	// Initialize the first entry of the finger table
	node.FingerTable[0].Id = node.Id.Bytes()
	node.FingerTable[0].NodeAddress = node.Address
	fmt.Printf("Node %s fingerTable[0]: Identifier=%x, Address=%s\n", node.Name, node.FingerTable[0].Id, node.FingerTable[0].NodeAddress)

	// Populate the remaining entries in the finger table
	for idx := 1; idx <= fingerTableLength; idx++ {
		offset := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(idx-1)), nil)
		calculatedIdentifier := new(big.Int).Add(node.Id, offset)
		calculatedIdentifier.Mod(calculatedIdentifier, chordSpace)

		node.FingerTable[idx].Id = calculatedIdentifier.Bytes()
		node.FingerTable[idx].NodeAddress = node.Address
	}
}

func (node *ChordNode) initSuccessorsAddr() { // REMOVE???
	successorsAddrNum := len(node.SuccessorsAddress)
	for i := 0; i < successorsAddrNum; i++ {
		node.SuccessorsAddress[i] = ""
	}
}

func (node *ChordNode) JoinChord(targetAddr string) error {
	log.Printf("Node at address %s attempting to join Chord network via %s", node.Address, targetAddr)
	node.PredecessoAddress = "" // Not needed?? TODO

	// RPC call to find the successor of the node
	var response FindSuccessorRPCReply
	if err := ChordCall(targetAddr, "ChordNode.FindSuccessorRPC", node.Id, &response); err != nil {
		return fmt.Errorf("failed to find successor: %w", err)
	}

	// Update the successor list
	node.SuccessorsAddress[0] = response.SuccessorAddress

	// Notify the successor about this node
	if notifyErr := ChordCall(node.SuccessorsAddress[0], "ChordNode.NotifyRPC", node.Address, &response); notifyErr != nil {
		return fmt.Errorf("failed to notify successor: %w", notifyErr)
	}

	return nil
}

func (node *ChordNode) DisplayState() {
	log.Println("========== Node State ==========")
	log.Printf("Name: %s\n", node.Name)
	log.Printf("Address: %s\n", node.Address)
	log.Printf("Identifier: %s\n", new(big.Int).SetBytes(node.Id.Bytes()))
	log.Printf("Predecessor: %s\n", node.PredecessoAddress)

	log.Println("Successors:")
	for idx, successor := range node.SuccessorsAddress {
		log.Printf("  Successor %d: %s\n", idx, successor)
	}

	log.Println("Finger Table:")
	for idx := 1; idx <= fingerTableLength; idx++ {
		entry := node.FingerTable[idx]
		id := new(big.Int).SetBytes(entry.Id)
		log.Printf("  Finger %d: ID=%s, Address=%s\n", idx, id, entry.NodeAddress)
	}

	log.Printf("Bucket: %v\n", node.Bucket)
	log.Printf("Backup: %v\n", node.Backup)
	log.Println("================================")
}
