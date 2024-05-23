package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	// Start the server and listen on TCP port 16379.
	fmt.Println("Listening on port :16379")
	listener, err := net.Listen("tcp", ":16379")
	if err != nil {
		fmt.Println("Failed to listen on port 16379:", err)
		return
	}
	defer listener.Close() // Ensure the listener is closed on function exit.

	// Initialize the Append-Only File (AOF) to store database modifications.
	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println("Failed to initialize AOF:", err)
		return
	}
	defer aof.Close() // Ensure the AOF file is closed on function exit.

	// Load the initial data into memory by processing each stored command.
	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk) // Convert command to uppercase.
		args := value.array[1:]                         // Extract arguments from the value.

		// Retrieve the handler function for the command.
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command received:", command)
			return
		}

		handler(args) // Execute the handler with arguments.
	})

	// Handle incoming connections in a loop.
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		// Process commands received from the connection.
		handleConnection(conn, aof)
	}
}

// handleConnection processes commands from the connection and responds.
func handleConnection(conn net.Conn, aof *Aof) {
	resp := NewResp(conn) // Initialize a new RESP (Redis Serialization Protocol) decoder.

	for {
		value, err := resp.Read()
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}

		if value.typ != "array" || len(value.array) == 0 {
			fmt.Println("Invalid request, expected non-empty array")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// Process the command.
		processCommand(conn, command, args, aof)
	}
}

// processCommand executes the command received via the network.
func processCommand(conn net.Conn, command string, args []Value, aof *Aof) {
	writer := NewWriter(conn) // Initialize a new RESP writer.

	handler, ok := Handlers[command]
	if !ok {
		fmt.Println("Invalid command:", command)
		writer.Write(Value{typ: "string", str: ""})
		return
	}

	// Handle special commands like "SET" or "HSET" that modify the database.
	if command == "SET" || command == "HSET" {
		// Manually constructing the array slice to include command and args.
		values := make([]Value, len(args)+1)
		values[0] = Value{typ: "bulk", bulk: command}
		copy(values[1:], args)
		aof.Write(Value{typ: "array", array: values})
	}

	result := handler(args) // Execute the handler and get the result.
	writer.Write(result)    // Write the result back to the client.
}
