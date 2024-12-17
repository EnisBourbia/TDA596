package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"unicode"
)

type IP struct {
	Query string
}

type Args struct {
	IPAddress   string
	Port        int
	JoinAddress string
	JoinPort    int
	Ts          int
	Tff         int
	Tcp         int
	R           int
	IdOverride  string
}

func handleArgs() *Args {
	address := flag.String("a", "", "The IP address to bind to and advertise.")
	port := flag.Int("p", -1, "The port to bind and listen on.")
	joinAddress := flag.String("ja", "", "The IP address of an existing Chord node to join.")
	joinPort := flag.Int("jp", -1, "The port of an existing Chord node to join.")
	ts := flag.Int("ts", 3000, "Time (ms) between stabilize calls.")
	tff := flag.Int("tff", 1000, "Time (ms) between fix fingers calls.")
	tcp := flag.Int("tcp", 3000, "Time (ms) between check predecessor calls.")
	r := flag.Int("r", 4, "Number of successors to maintain.")
	idOverride := flag.String("i", "", "Override the computed ID with a custom identifier.")

	flag.Parse()

	// Validate required parameters
	if *address == "" || *port == -1 {
		fmt.Println("-a and -p must be specified.")
		os.Exit(1)
	}
	if (*joinPort != -1 && *joinAddress == "") || (*joinPort == -1 && *joinAddress != "") {
		fmt.Println("If either --ja or --jp is specified, the other must be specified.")
		os.Exit(1)
	}
	if *ts < 1 || *ts > 60000 {
		fmt.Println("invalid ts argument:", *ts)
		os.Exit(1)
	}
	if *tff < 1 || *tff > 60000 {
		fmt.Println("invalid tff argument:", *tff)
		os.Exit(1)
	}
	if *tcp < 1 || *tcp > 60000 {
		fmt.Println("invalid tcp argument:", *tcp)
		os.Exit(1)
	}
	if *r < 1 || *r > 32 {
		fmt.Println("invalid r argument:", *r)
		os.Exit(1)
	}

	if *idOverride != "" {
		notValid := false
		for _, l := range *idOverride {
			if !((l >= 'a' && l <= 'f') || (l >= 'A' && l <= 'F') || unicode.IsDigit(l)) {
				notValid = true
				break
			}
		}
		if notValid || len(*idOverride) != 40 {
			fmt.Println("id should be 40 characters of [0-9a-fA-F]:", *idOverride)
			os.Exit(1)
		}
	}

	return &Args{
		IPAddress:   *address,
		Port:        *port,
		JoinAddress: *joinAddress,
		JoinPort:    *joinPort,
		Ts:          *ts,
		Tff:         *tff,
		Tcp:         *tcp,
		R:           *r,
		IdOverride:  *idOverride,
	}
}

func fetchLocalIPAddress() string {
	// Connect to Google's public DNS server to determine local IP address
	connection, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatalf("Error determining local IP address: %v", err)
	}
	defer connection.Close()

	// Extract and return the local IP address
	addr := connection.LocalAddr().(*net.UDPAddr)
	return addr.IP.String()
}

func SHash(s string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(s))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

func getMachineIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", fmt.Errorf("failed to dial remote host: %w", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

func isInRange(start, el, end *big.Int, inclusive bool) bool {
	if start.Cmp(end) < 0 { // Case: start < end
		if inclusive {
			return (el.Cmp(start) > 0 && el.Cmp(end) <= 0)
		}
		return el.Cmp(start) > 0 && el.Cmp(end) < 0
	}

	// Case: end < start (wrap-around)
	if inclusive && el.Cmp(end) == 0 {
		return true
	}
	return el.Cmp(start) > 0 || el.Cmp(end) < 0
}

func getLocalAddress() string {
	// Obtain the worker's public IP address using an external API
	req, err := http.Get("http://ip-api.com/json/")
	if err != nil {
		log.Fatalf("Could not get IP: %v", err)
	}
	defer req.Body.Close()

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Fatalf("Could not read IP response: %v", err)
	}

	var ip IP
	json.Unmarshal(body, &ip)
	return ip.Query // Extract the public IP
}

// Securely encrypts the given file content using the node's public key
func (node *ChordNode) Encrypt(content []byte) []byte {
	pubKey := node.PublicEncryptionKey
	encryptedText, err := rsa.EncryptPKCS1v15(rand.Reader, pubKey, content)
	if err != nil {
		log.Println("Failed to encrypt content:", err)
		return nil
	}
	return encryptedText // Encrypted content
}

// Decrypt securely decrypts the given encrypted file content using the node's private key
func (node *ChordNode) Decrypt(cipherText []byte) []byte {
	privKey := node.PrivateEncryptionKey
	plainText, err := rsa.DecryptPKCS1v15(rand.Reader, privKey, cipherText)
	if err != nil {
		log.Println("Failed to decrypt content:", err)
		return nil
	}
	return plainText // Decrypted content
}

func (node *ChordNode) generateRSAKeys(keySize int) {
	// Generate RSA private key
	privKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		log.Printf("[generateRSAKeys] Error generating private key for Node %s (N%s): %v\n", node.Name, node.Id.String(), err)
		return
	}
	node.PrivateEncryptionKey = privKey
	node.PublicEncryptionKey = &privKey.PublicKey

	// Define the node folder path
	nodeDir := fmt.Sprintf("../files/N%s", node.Id.String())

	// Save the private key to a PEM file
	privateKeyDER := x509.MarshalPKCS1PrivateKey(privKey)
	privateKeyBlock := &pem.Block{
		Type:    fmt.Sprintf("N%s-private Key", node.Id.String()),
		Headers: nil,
		Bytes:   privateKeyDER,
	}
	privateKeyPath := fmt.Sprintf("%s/private.pem", nodeDir)
	if err := savePEMToFile(privateKeyPath, privateKeyBlock); err != nil {
		log.Printf("[generateRSAKeys] Failed to save private key for Node %s: %v\n", node.Name, err)
	}

	// Save the public key to a PEM file
	publicKeyDER, err := x509.MarshalPKIXPublicKey(node.PublicEncryptionKey)
	if err != nil {
		log.Printf("[generateRSAKeys] Error marshalling public key for Node %s: %v\n", node.Name, err)
		return
	}
	publicKeyBlock := &pem.Block{
		Type:    fmt.Sprintf("N%s-public Key", node.Id.String()),
		Headers: nil,
		Bytes:   publicKeyDER,
	}
	publicKeyPath := fmt.Sprintf("%s/public.pem", nodeDir)
	if err := savePEMToFile(publicKeyPath, publicKeyBlock); err != nil {
		log.Printf("[generateRSAKeys] Failed to save public key for Node %s: %v\n", node.Name, err)
	}
}

// Helper function to save a PEM block to a file
func savePEMToFile(filePath string, block *pem.Block) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("unable to create file %s: %v", filePath, err)
	}
	defer file.Close()

	if err := pem.Encode(file, block); err != nil {
		return fmt.Errorf("failed to encode PEM block to file %s: %v", filePath, err)
	}
	return nil
}

// Encrypt content with AES
func EncryptWithAES(key, content []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	encryptedContent := gcm.Seal(nonce, nonce, content, nil)
	return encryptedContent, nil
}

// Decrypt content with AES
func DecryptWithAES(key, encryptedContent []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(encryptedContent) < nonceSize {
		return nil, errors.New("invalid encrypted content: too short")
	}

	// Extract nonce and ciphertext
	nonce, ciphertext := encryptedContent[:nonceSize], encryptedContent[nonceSize:]

	// Decrypt the content
	return gcm.Open(nil, nonce, ciphertext, nil)
}
