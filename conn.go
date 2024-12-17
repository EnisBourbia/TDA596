package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
)

var SharedAESKey = []byte("d3882475848977c627e5b696e35d150b")

type RPCFileStorageResponse struct {
	Successful bool
	Err        error
	Backup     bool
}

type NotifyReply struct {
	Success bool
}

type FileFetchArgs struct {
	FileName string
}

type FileFetchReply struct {
	Content []byte
	Found   bool
}

type GetIDRPCReply struct {
	Identifier *big.Int
}

func (node *ChordNode) GetIDRPC(none string, reply *GetIDRPCReply) error {
	reply.Identifier = node.Id
	return nil
}

type NotifyRPCReply struct {
	Success bool
}

// change the predecessor of the node to addr
func (node *ChordNode) notify(addr string) (bool, error) {
	if node.PredecessoAddress != "" {
		var getPredecessorIDRPCReply GetIDRPCReply
		err := ChordCall(node.PredecessoAddress, "ChordNode.GetIDRPC", "", &getPredecessorIDRPCReply)
		if err != nil {
			log.Println("Failed to get the name of predecessor:", err)
			return false, err
		}
		predecessorID := getPredecessorIDRPCReply.Identifier

		var getAddrIDRPCReply GetIDRPCReply
		err = ChordCall(addr, "ChordNode.GetIDRPC", "", &getAddrIDRPCReply)
		if err != nil {
			log.Println("Failed to get the name of predecessor:", err)
			return false, err
		}
		addrID := getAddrIDRPCReply.Identifier

		if isInRange(predecessorID, addrID, node.Id, false) {
			node.PredecessoAddress = addr
			// log.Println(node.Name, "'s Predecessor is set to ", addr)
			return true, nil
		} else {
			return false, nil
		}
	} else {
		node.PredecessoAddress = addr
		// log.Println(node.Name, "'s Predecessor is set to ", addr)
		return true, nil
	}
}

func (node *ChordNode) NotifyRPC(addr string, reply *NotifyReply) error {
	// logic remains the same, just changed struct type name
	if node.SuccessorsAddress[0] != node.Address {
		node.moveFiles(addr)
	}

	reply.Success, _ = node.notify(addr)
	return nil
}

type GetSuccessorListRPCReply struct {
	SuccessorList []string
}

func (node *ChordNode) GetSuccessorListRPC(none *struct{}, reply *GetSuccessorListRPCReply) error {
	reply.SuccessorList = node.SuccessorsAddress
	return nil
}

type GetPredecessorRPCReply struct {
	PredecessorAddr string
}

func (node *ChordNode) GetPredecessorRPC(none *struct{}, reply *GetPredecessorRPCReply) error {
	reply.PredecessorAddr = node.PredecessoAddress
	if reply.PredecessorAddr == "" {
		return errors.New("predecessor is empty")
	} else {
		return nil
	}
}

type LookupReply struct {
	Found         bool
	SuccessorAddr string
}

func Lookup(id *big.Int, startNode string) string {
	//log.Println("---------------Invocation of Lookup start------------------")
	id.Mod(id, chordSpace)
	next := startNode
	flag := false
	result := FindSuccessorRPCReply{}
	if !flag {
		err := ChordCall(next, "ChordNode.FindSuccessorRPC", id, &result)
		if err != nil {
			log.Printf("[Lookup] Find successor rpc error: %s\n", err)
		}
		flag = result.Found
		next = result.SuccessorAddress
	}
	return next
}

/*
FindSuccessorRPCReply function reply and its implementation
*/

type FindSuccessorRPCReply struct {
	Found            bool
	SuccessorAddress string
}

type GetAddrRPCReply struct {
	Addr string
}

func (node *ChordNode) FindSuccessorRPC(id *big.Int, reply *FindSuccessorRPCReply) error {
	//log.Println("---------------invocation of FindSuccessor----------------")
	var successorAddr string
	getAddrRPCReply := GetAddrRPCReply{}
	err := ChordCall(node.SuccessorsAddress[0], "ChordNode.GetAddrRPC", "", &getAddrRPCReply)
	if err != nil {
		log.Printf("[FindSuccessorRPC] Get AddrRPC error: %s\n", err)
	}

	successorAddr = getAddrRPCReply.Addr
	successorId := SHash(successorAddr)
	successorId.Mod(successorId, chordSpace)
	id.Mod(id, chordSpace)

	flag := isInRange(node.Id, id, successorId, true)

	reply.Found = false
	if flag {
		// the id is between node and its successor
		reply.Found = true
		reply.SuccessorAddress = node.SuccessorsAddress[0]
	} else {
		// find the successor from fingertable
		successorAddr = node.LookupFingerTable(id)
		findSuccessorRPCReply := FindSuccessorRPCReply{}
		err = ChordCall(successorAddr, "ChordNode.FindSuccessorRPC", id, &findSuccessorRPCReply)
		if err != nil {
			log.Printf("[FindSuccessorRPC] Find successor rpc error: %s", err)
		}
		reply.Found = findSuccessorRPCReply.Found
		reply.SuccessorAddress = findSuccessorRPCReply.SuccessorAddress
	}
	return nil
}

func (node *ChordNode) GetAddrRPC(none string, reply *GetAddrRPCReply) error {
	reply.Addr = node.Address
	return nil
}

type SetPredecessorRPCReply struct {
	Success bool
}

func (node *ChordNode) SetPredecessorRPC(predecessorAddr string, reply *SetPredecessorRPCReply) error {
	//fmt.Println("-------------- Invoke SetPredecessorRPC function ------------")
	node.PredecessoAddress = predecessorAddr
	reply.Success = true
	return nil
}

func (node *ChordNode) LookupFingerTable(id *big.Int) string {
	//log.Println("--------------invocation of LookupFingerTable--------------")
	size := len(node.FingerTable)
	for i := size - 1; i >= 1; i-- {
		getAddrRPCReply := GetAddrRPCReply{}
		err := ChordCall(node.FingerTable[i].NodeAddress, "ChordNode.GetAddrRPC", "", &getAddrRPCReply)
		if err != nil {
			log.Printf("[LookupFingerTable] Get addrRPC error: %s\n", err)
		}

		fingerId := SHash(getAddrRPCReply.Addr)
		fingerId.Mod(fingerId, chordSpace)
		flag := isInRange(node.Id, fingerId, id, false) // Todo, why is false. e.g. 42  (8, 54]
		if flag {
			return node.FingerTable[i].NodeAddress
		}
	}
	return node.SuccessorsAddress[0]
}

type GetPublicKeyRPCReply struct {
	Public_Key *rsa.PublicKey
}

func (node *ChordNode) GetPublicKeyRPC(none string, reply *GetPublicKeyRPCReply) error {
	reply.Public_Key = node.PublicEncryptionKey
	return nil
}

type FileStructure struct {
	Id      *big.Int
	Name    string // file name e.g. "../files/" + node.Name + "/upload/"
	Content []byte
}

// func StoreFile(fileName string, node *ChordNode) error {
// 	// find which node should this file stored
// 	key := SHash(fileName)
// 	addr := Lookup(key, node.Address)
// 	// read the file and upload it to addr
// 	filePath := "../files/" + "N" + node.Id.String() + "/upload/"
// 	filePath += fileName
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		log.Println("The file cannot be opened!")
// 	}
// 	content, err := io.ReadAll(file)
// 	if err != nil {
// 		log.Println("The file cannot be read!")
// 	}
// 	defer file.Close()

// 	newFile := FileStructure{}
// 	newFile.Name = fileName
// 	newFile.Id = key
// 	newFile.Id.Mod(newFile.Id, chordSpace)
// 	newFile.Content = content

// 	//encrypt the file
// 	var getPublicKeyRPCReply GetPublicKeyRPCReply
// 	err = ChordCall(addr, "ChordNode.GetPublicKeyRPC", "", &getPublicKeyRPCReply)
// 	if err != nil {
// 		log.Println(err)
// 		return err
// 	}

// 	if getPublicKeyRPCReply.Public_Key == nil {
// 		return fmt.Errorf("public key is nil, cannot encrypt file content")
// 	}

// 	if node.EncryptionEnabled {
// 		newFile.Content, err = rsa.EncryptPKCS1v15(rand.Reader, getPublicKeyRPCReply.Public_Key, newFile.Content)
// 		if err != nil {
// 			return fmt.Errorf("encryption failed: %v", err)

// 		}
// 	}

// 	// send storefile rpc
// 	reply := RPCStoreFileResponse{}
// 	reply.Backup = false
// 	err = ChordCall(addr, "ChordNode.StoreFileRPC", newFile, &reply)
// 	if err != nil {
// 		log.Println(err)
// 	}

// 	return err
// }

// func StoreFile(fullPath string, fileName string, node *ChordNode) error {
// 	// find which node should this file should be stored on
// 	key := SHash(fileName)
// 	addr := Lookup(key, node.Address)

// 	// read the file directly from the provided fullPath
// 	file, err := os.Open(fullPath)
// 	if err != nil {
// 		return fmt.Errorf("The file cannot be opened: %v", err)
// 	}
// 	content, err := io.ReadAll(file)
// 	if err != nil {
// 		file.Close()
// 		return fmt.Errorf("The file cannot be read: %v", err)
// 	}
// 	file.Close()

// 	newFile := FileStructure{
// 		Name:    fileName,
// 		Id:      key,
// 		Content: content,
// 	}
// 	newFile.Id.Mod(newFile.Id, chordSpace)

// 	// encrypt the file
// 	var getPublicKeyRPCReply GetPublicKeyRPCReply
// 	err = ChordCall(addr, "ChordNode.GetPublicKeyRPC", "", &getPublicKeyRPCReply)
// 	if err != nil {
// 		return fmt.Errorf("failed to get public key from %s: %v", addr, err)
// 	}

// 	if node.EncryptionEnabled {
// 		if getPublicKeyRPCReply.Public_Key == nil {
// 			return fmt.Errorf("public key is nil, cannot encrypt")
// 		}
// 		newFile.Content, err = rsa.EncryptPKCS1v15(rand.Reader, getPublicKeyRPCReply.Public_Key, newFile.Content)
// 		if err != nil {
// 			return fmt.Errorf("encryption failed: %v", err)
// 		}
// 	}

// 	// send storefile rpc
// 	reply := RPCStoreFileResponse{}
// 	reply.Backup = false
// 	err = ChordCall(addr, "ChordNode.StoreFileRPC", newFile, &reply)
// 	if err != nil {
// 		return fmt.Errorf("ChordCall to StoreFileRPC failed: %v", err)
// 	}

// 	return nil
// }

type GetFileRPCArgs struct {
	FileName string
}

type GetFileRPCReply struct {
	Content []byte
	Found   bool
}

func (node *ChordNode) GetFileRPC(args GetFileRPCArgs, reply *FileFetchReply) error {
	for _, fileName := range node.Bucket {
		if fileName == args.FileName {
			filePath := "../files/N" + node.Id.String() + "/chord_storage/" + fileName
			encryptedContent, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read stored file: %v", err)
			}

			// Decrypt the content with the shared AES key
			decryptedContent, err := DecryptWithAES(SharedAESKey, encryptedContent)
			if err != nil {
				return fmt.Errorf("failed to decrypt file content: %v", err)
			}

			reply.Content = decryptedContent
			reply.Found = true
			return nil
		}
	}

	reply.Found = false
	return nil
}

// StoreFile encrypts the file using AES before sending it for storage
func StoreFile(fullPath string, fileName string, node *ChordNode) error {
	// Find the target node
	key := SHash(fileName)
	addr := Lookup(key, node.Address)

	// Read the file
	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("the file cannot be opened: %v", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("the file cannot be read: %v", err)
	}
	file.Close()

	// Encrypt the content with AES
	encryptedContent, err := EncryptWithAES(SharedAESKey, content)
	if err != nil {
		return fmt.Errorf("failed to encrypt file content: %v", err)
	}

	newFile := FileStructure{
		Name:    fileName,
		Id:      key,
		Content: encryptedContent,
	}
	newFile.Id.Mod(newFile.Id, chordSpace)

	// Send the encrypted file
	reply := RPCStoreFileResponse{}
	reply.Backup = false
	err = ChordCall(addr, "ChordNode.StoreFileRPC", newFile, &reply)
	if err != nil {
		return fmt.Errorf("ChordCall to StoreFileRPC failed: %v", err)
	}

	return nil
}

func (node *ChordNode) StoreFileRPC(f FileStructure, reply *RPCFileStorageResponse) error {
	flag := node.storeFile(f, reply.Backup)
	reply.Successful = flag
	if flag {
		log.Println("File storage success!")
	} else {
		log.Println("File storage error!")
		return errors.New("File storage error!")
	}
	return nil
}

func (node *ChordNode) storeFile(f FileStructure, backUp bool) bool {
	idStr := f.Id.String()

	if backUp {
		if _, exists := node.Backup[idStr]; exists {
			log.Println("File already exists in Backup")
			return false
		}
		node.Backup[idStr] = f.Name
		log.Println("Backup updated:", node.Backup)
	} else {
		if _, exists := node.Bucket[idStr]; exists {
			log.Println("File already exists in Bucket")
			return false
		}
		node.Bucket[idStr] = f.Name
		log.Println("Bucket updated:", node.Bucket)
	}

	filePath := "../files/N" + node.Id.String() + "/chord_storage/" + f.Name
	file, err := os.Create(filePath)
	if err != nil {
		log.Println("Error creating file:", err)
		return false
	}
	defer file.Close()

	// Write AES-encrypted content directly
	_, err = file.Write(f.Content)
	if err != nil {
		log.Println("Error writing file content:", err)
		return false
	}

	return true
}

type CheckFileExistRPCReply struct {
	Exist bool
}

func (node *ChordNode) CheckFileExistRPC(fileName string, reply *CheckFileExistRPCReply) error {
	//log.Println("----------------invocation of checkfileexistRPC---------------")
	for _, value := range node.Bucket {
		if fileName == value {
			reply.Exist = true
			return nil
		}
	}
	reply.Exist = false
	return nil
}

type DeleteSuccessorBackupRPCReply struct {
	Success bool
}

func (node *ChordNode) DeleteSuccessorBackupRPC(args interface{}, reply *DeleteSuccessorBackupRPCReply) error {
	reply.Success = node.deleteSuccessorBackupRPC()
	return nil
}

func (node *ChordNode) deleteSuccessorBackupRPC() bool {
	for key, _ := range node.Backup {
		// we just remove the reference to the key, but the file still exists in local disk. It will be cleaned later
		delete(node.Backup, key)
	}
	return true
}

type SuccessorStoreFileRPCReply struct {
	Successor bool
	Error     error
}

func (node *ChordNode) SuccessorStoreFileRPC(f FileStructure, reply *SuccessorStoreFileRPCReply) error {
	reply.Successor = node.successorStoreFile(f)
	if !reply.Successor {
		reply.Error = errors.New("SuccessorStoreFileRPC error!")
		return reply.Error
	} else {
		reply.Error = nil
		return nil
	}
}

func (node *ChordNode) successorStoreFile(f FileStructure) bool {
	filePath := "../files/N" + node.Id.String() + "/chord_storage/" + f.Name

	file, err := os.Create(filePath)
	if err != nil {
		log.Println("[successorStoreFile] Create file error: ", err)
		return false
	}
	defer file.Close()

	// Store encrypted content directly
	_, err = file.Write(f.Content)
	if err != nil {
		log.Println("[successorStoreFile] File write error: ", err)
		return false
	}

	node.Backup[f.Id.String()] = f.Name
	return true
}

func (node *ChordNode) moveFiles(addr string) {
	var getIdReply GetIDRPCReply
	err := ChordCall(addr, "ChordNode.GetIDRPC", "", &getIdReply)
	if err != nil {
		log.Println("Failed to get the id:", err)
		return
	}

	// Iterate local bucket
	for keyStr, value := range node.Bucket {
		newFile := FileStructure{
			Id:   new(big.Int).SetBytes([]byte(keyStr)), // Ensure correct ID format
			Name: value,
		}

		// Read the already encrypted file content
		filePath := "../files/N" + node.Id.String() + "/chord_storage/" + value
		encryptedContent, err := os.ReadFile(filePath)
		if err != nil {
			log.Println("Error opening file for backup:", err)
			continue
		}

		// Use the already encrypted content
		newFile.Content = encryptedContent

		// Send the file to the successor
		backupReply := SuccessorStoreFileRPCReply{}
		err = ChordCall(node.SuccessorsAddress[0], "ChordNode.SuccessorStoreFileRPC", newFile, &backupReply)
		if err != nil || backupReply.Error != nil {
			log.Println("Failed to back up file:", err)
		} else {
			log.Printf("File %s backed up successfully to %s\n", value, node.SuccessorsAddress[0])
		}
	}
}

// EncryptFileContent performs hybrid encryption
func EncryptFileContent(publicKey *rsa.PublicKey, content []byte) ([]byte, []byte, error) {
	// Step 1: Generate a random AES key
	aesKey := make([]byte, 32) // 256-bit AES key
	if _, err := rand.Read(aesKey); err != nil {
		return nil, nil, err
	}

	// Step 2: Encrypt the content with AES
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}

	encryptedContent := gcm.Seal(nonce, nonce, content, nil)

	// Step 3: Encrypt the AES key with RSA
	encryptedAESKey, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey, aesKey)
	if err != nil {
		return nil, nil, errors.New("failed to encrypt AES key with RSA")
	}

	return encryptedAESKey, encryptedContent, nil
}
