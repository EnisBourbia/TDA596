package main

import (
	"fmt"
	"log"
	"net/rpc/jsonrpc"
	"strings"
)

func ChordCall(targetNodeAddr, serviceMethod string, args, reply interface{}) error {
	// Validate the target node address format
	if parts := strings.Split(targetNodeAddr, ":"); len(parts) != 2 {
		log.Printf("Invalid target node address: %s\n", targetNodeAddr)
		return fmt.Errorf("invalid address format: %s", targetNodeAddr)
	}

	// Establish a connection to the target node
	connection, err := jsonrpc.Dial("tcp", targetNodeAddr)
	if err != nil {
		log.Printf("Failed to dial %s for method %s: %v\n", targetNodeAddr, serviceMethod, err)
		return err
	}
	defer connection.Close()

	// Invoke the service method
	if err := connection.Call(serviceMethod, args, reply); err != nil {
		log.Printf("Error calling method %s: %v\n", serviceMethod, err)
		return err
	}

	return nil
}
