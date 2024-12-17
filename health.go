package main

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/rpc/jsonrpc"
	"os"
	"strings"
)

func (node *ChordNode) stabilize() error {
	//firstly, update successor list
	//the successor list of node: successor[0] is the next server node that near active node
	//1-(n-1) are the first (n-1) items of the successor list of successor[0]
	//if successor[0] is dead, remove it and shift the successor list to the upper
	var getSuccessorListRPCReply GetSuccessorListRPCReply
	err := ChordCall(node.SuccessorsAddress[0], "ChordNode.GetSuccessorListRPC", struct{}{}, &getSuccessorListRPCReply)
	if err != nil {
		log.Println(err)
	}
	successorListReply := getSuccessorListRPCReply.SuccessorList
	if err == nil {
		for i := 0; i < len(successorListReply)-1; i++ {
			node.SuccessorsAddress[i+1] = successorListReply[i]

		}
	} else {
		log.Println("Failed to get successor list", err)
		if node.SuccessorsAddress[0] == "" {
			log.Println("successorList[0] is empty, use itself as successorList[0]")
			node.SuccessorsAddress[0] = node.Address
		} else {
			//successorList[0] is dead, remove it and shift the list to the upper
			for i := 0; i < len(node.SuccessorsAddress); i++ {
				if i == len(node.SuccessorsAddress)-1 {
					node.SuccessorsAddress[i] = ""
				} else {
					node.SuccessorsAddress[i] = node.SuccessorsAddress[i+1]
				}
			}

		}
	}

	//change the active node's successor to the successor[0]'s predecessor
	//find the predecessor of the node's successor
	var getPredecessorRPCReply GetPredecessorRPCReply
	err = ChordCall(node.SuccessorsAddress[0], "ChordNode.GetPredecessorRPC", struct{}{}, &getPredecessorRPCReply)
	if err != nil {
		log.Println(err)
	}
	//if the predecessor is not the active node
	//change the node's successor to the newer predecessor of pre-successor of the active node and notify
	if err == nil {
		var getSuccessorIDRPCReply GetIDRPCReply
		err = ChordCall(node.SuccessorsAddress[0], "ChordNode.GetIDRPC", "", &getSuccessorIDRPCReply)
		if err != nil {
			log.Println("Failed to get successor[0] id")
			return err
		}
		successorID := getSuccessorIDRPCReply.Identifier

		predecessorAddr := getPredecessorRPCReply.PredecessorAddr
		var getPredecessorIDRPCReply GetIDRPCReply
		err = ChordCall(predecessorAddr, "ChordNode.GetIDRPC", "", &getPredecessorIDRPCReply)
		if err != nil {
			log.Println("Failed to get predecessor id")
			return err
		}
		predecessorID := getPredecessorIDRPCReply.Identifier
		if predecessorAddr != "" && isInRange(node.Id, predecessorID, successorID, false) {
			node.SuccessorsAddress[0] = predecessorAddr
		}

	}
	//notify
	err = ChordCall(node.SuccessorsAddress[0], "ChordNode.NotifyRPC", node.Address, &NotifyRPCReply{})
	if err != nil {
		log.Printf("[stabilize] Notify rpc error: %s\n", err)
	}
	// 1. First delete successor's backup
	// 2. Copy current bucket to successor's backup(do not do it if there is one node left)
	deleteSuccessorBackupRPCReply := DeleteSuccessorBackupRPCReply{}
	err = ChordCall(node.SuccessorsAddress[0], "ChordNode.DeleteSuccessorBackupRPC", struct{}{}, &deleteSuccessorBackupRPCReply)
	if err != nil {
		log.Println("Delete successor's backup error: ", err)
		return err
	}

	if node.SuccessorsAddress[0] == node.Address {
		return nil
	}
	for key, value := range node.Bucket {
		newFile := FileStructure{}
		fileID := new(big.Int)
		if _, success := fileID.SetString(key, 10); !success {
			log.Printf("Failed to convert string to *big.Int: %s\n", key)
			continue
		}
		newFile.Id = fileID
		newFile.Name = value
		filePath := "../files/" + "N" + node.Id.String() + "/chord_storage/" + value
		file, err := os.Open(filePath)
		if err != nil {
			log.Println("Open node's bucket file error: ", err)
			return err
		}
		defer file.Close()
		content, err := io.ReadAll(file)
		if err != nil {
			log.Println("Read node's bucket file error: ", err)
			return err
		}
		//encrypt the file
		newFile.Content = content
		//encrypt the content
		var getPublicKeyRPCReply GetPublicKeyRPCReply
		err = ChordCall(node.SuccessorsAddress[0], "ChordNode.GetPublicKeyRPC", "", &getPublicKeyRPCReply)
		if err != nil {
			log.Println(err)
		}
		if node.EncryptionEnabled {
			newFile.Content, _ = rsa.EncryptPKCS1v15(rand.Reader, getPublicKeyRPCReply.Public_Key, newFile.Content)
		}

		successorStoreFileReply := SuccessorStoreFileRPCReply{}
		err = ChordCall(node.SuccessorsAddress[0], "ChordNode.SuccessorStoreFileRPC", newFile, &successorStoreFileReply)
		if err != nil {
			log.Println(err)
		}
		if successorStoreFileReply.Error != nil || err != nil {
			log.Println("[stabilize] Store files to successor error: ", successorStoreFileReply.Error, " and: ", err)
			return nil
		}

	}
	// Clean the redundant file in successor's backup
	node.cleanRedundantFile()

	return nil
}

func (node *ChordNode) cleanRedundantFile() {
	// Read all local storage files
	filePath := "../files/" + "N" + node.Id.String() + "/chord_storage"
	files, err := os.ReadDir(filePath)
	if err != nil {
		log.Println("[cleanRedundantFile] Read directory error: ", err)
		return
	}
	for _, file := range files {
		fileName := file.Name()
		fileId := SHash(fileName)
		fileId.Mod(fileId, chordSpace)

		inBucket := false
		inBackup := false
		for id, _ := range node.Bucket {
			if id == fileId.String() {
				inBucket = true
			}
		}

		for id, _ := range node.Backup {
			if id == (fileId.String()) {
				inBackup = true
			}
		}

		if !inBackup && !inBucket {
			// The file is not in backup and bucket, delete it
			path := filePath + "/" + fileName
			err = os.Remove(path)
			if err != nil {
				log.Printf("[cleanRedundantFile] Cannot remove the file[%s]: ", path)
				return
			}
		}
	}
}

// input the identifier of the node, 0-63
// return the start of the interval
func (node *ChordNode) FingerStart(nodeId int) *big.Int {
	id := node.Id
	id = new(big.Int).Add(id, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(nodeId)-1), nil))
	return new(big.Int).Mod(id, chordSpace)
}

// FixFingers updates finger table
func (node *ChordNode) FixFingers() error {

	node.NextFinger += 1
	if node.NextFinger > chordBits {
		node.NextFinger = 1
	}
	// n + 2^next-1, this key is a file id
	//todo:why not using node.FingerTable[node.nextFinger].Identifier
	//
	key := new(big.Int).Add(node.Id, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(node.NextFinger)-1), nil))
	key.Mod(key, chordSpace)

	// find the successor of the key
	next := Lookup(key, node.Address)

	node.FingerTable[node.NextFinger].NodeAddress = next
	node.FingerTable[node.NextFinger].Id = key.Bytes()

	return nil
}

// check whether predecessor has failed
func (node *ChordNode) checkPredecessor() error {
	pred := node.PredecessoAddress
	if pred != "" {
		ip := strings.Split(pred, ":")[0]
		port := strings.Split(pred, ":")[1]

		// ip = NAT(ip)

		predAddr := ip + ":" + port
		_, err := jsonrpc.Dial("tcp", predAddr)
		if err != nil {
			fmt.Printf("Predecessor %s has failed\n", pred)
			node.PredecessoAddress = ""
			for k, v := range node.Backup {
				if v != "" {
					node.Bucket[k] = v
				}
			}

		}
	}
	return nil
}
