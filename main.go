package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const KeySizeBits = 160

type PeriodicTaskRunner struct {
	delay  time.Duration
	ticker time.Ticker
	quit   chan int
}

func (r *PeriodicTaskRunner) Begin(task func()) {
	r.ticker = *time.NewTicker(r.delay) // Initialize the ticker

	go func() {
		for {
			select {
			case <-r.ticker.C: // Trigger the task on each tick
				go func() {
					go task()
				}()
			case <-r.quit: // Stop the ticker when quit signal is received
				r.ticker.Stop()
				return
			}
		}
	}()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	args := handleArgs()
	log.Println(args)
	// Create chord node
	node := CreateNode(*args)
	ipAddress := args.IPAddress + ":" + strconv.Itoa(args.Port)

	addr, err := net.ResolveTCPAddr("tcp", ipAddress)
	if err != nil {
		log.Fatalln("ResolveTCPAddr failed:", err.Error())
	}
	rpc.Register(node)
	listener, err := net.Listen("tcp", addr.String())
	if err != nil {
		log.Fatalln("ListenTCP failed:", err.Error())
	}
	defer listener.Close()

	go func(listener net.Listener) {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Accept failed:", err.Error())
				continue
			}
			go jsonrpc.ServeConn(conn)
		}
	}(listener)

	if args.JoinAddress != "" && args.JoinPort > 0 {
		log.Println("Joining existing Chord network at:", args.JoinAddress, args.JoinPort)
		networkAddress := args.JoinAddress + ":" + strconv.Itoa(args.JoinPort)
		node.JoinChord(networkAddress)
	} else {
		log.Println("No valid join address provided. Creating a new Chord network.")
		node.NewChord()
	}

	runnerStabilize := PeriodicTaskRunner{
		delay: time.Duration(args.Ts) * time.Millisecond,
		quit:  make(chan int),
	}

	runnerStabilize.Begin(func() {
		node.stabilize()
	})

	runnerFixFinger := PeriodicTaskRunner{
		delay: time.Duration(args.Tff) * time.Millisecond,
		quit:  make(chan int),
	}
	runnerFixFinger.Begin(func() {
		node.FixFingers()
	})

	runnerCheckPredecessor := PeriodicTaskRunner{
		delay: time.Duration(args.Tcp) * time.Millisecond,
		quit:  make(chan int),
	}
	runnerCheckPredecessor.Begin(func() {
		node.checkPredecessor()
	})

	// Read input from stdin
	reader := bufio.NewReader(os.Stdin)
	for {
		log.Println("Please enter your command(Lookup/StoreFile/PrintState/Quit)...")
		command, _ := reader.ReadString('\n')
		command = strings.ToLower(strings.TrimSpace(command))
		if command == "lookup" {
			log.Println("Please enter the file you want to look up...")
			fileName, _ := reader.ReadString('\n')
			fileName = strings.TrimSpace(fileName)
			// hash the file name to find the key
			key := SHash(fileName)
			targetAddr := Lookup(key, node.Address)
			log.Println("The node that could have the required data: ", targetAddr)

			// Check if the file exists
			checkFileExistRPCReply := CheckFileExistRPCReply{}
			err = ChordCall(targetAddr, "ChordNode.CheckFileExistRPC", fileName, &checkFileExistRPCReply)
			if err != nil {
				log.Println("Check file exist fail..", err)
				continue
			}

			if checkFileExistRPCReply.Exist {
				var getAddrRPCReply GetAddrRPCReply
				err = ChordCall(targetAddr, "ChordNode.GetAddrRPC", "", &getAddrRPCReply)
				if err != nil {
					log.Println("Chord Call failed! ")
					continue
				}

				id := SHash(targetAddr)
				id.Mod(id, chordSpace)
				log.Printf("The file is stored at node: %d ,address:port is :%s\n", id, targetAddr)

				// Now retrieve the file content
				getFileArgs := FileFetchArgs{FileName: fileName}
				var getFileReply FileFetchReply
				err = ChordCall(targetAddr, "ChordNode.GetFileRPC", getFileArgs, &getFileReply)
				if err != nil {
					log.Printf("Failed to retrieve file content: %v\n", err)
					continue
				}

				if getFileReply.Found {
					log.Printf("File contents:\n%s\n", string(getFileReply.Content))
				} else {
					log.Println("File not found after existence check (unexpected).")
				}
			} else {
				log.Println("The file is not stored at this node: ", targetAddr)
			}
		} else if command == "storefile" {
			log.Println("Please enter the full path of the file you want to upload...")
			fullPath, _ := reader.ReadString('\n')
			fullPath = strings.TrimSpace(fullPath)

			// Extract fileName from fullPath if needed
			// For example, if fullPath = /home/user/Hello.txt
			fileName := filepath.Base(fullPath)

			err = StoreFile(fullPath, fileName, node)
			if err != nil {
				log.Println(err)
			} else {
				log.Println("File storage success!")
			}

		} else if command == "printstate" {
			node.DisplayState()
		} else if command == "quit" {
			runnerStabilize.quit <- 1
			runnerFixFinger.quit <- 1
			runnerCheckPredecessor.quit <- 1
			os.Exit(0)
		} else {
			log.Println("Invalid command! Please enter your command again(Lookup/StoreFile/PrintState/Quit)...")
		}
	}
}
