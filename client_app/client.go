package main

import (
	"fmt"
	"hms/algorithms"
	"hms/models"
	"log"
	"net"
	"net/rpc"
	"os"
)

// ClientRA handles Ricart-Agrawala RPCs for client coordination
type ClientRA struct {
	ra *algorithms.RicartAgrawala
}

func (c *ClientRA) RequestPermission(req models.RicartRequest, reply *models.RicartReply) error {
	c.ra.HandleRequest(req, reply)
	return nil
}

func (c *ClientRA) ReceiveReply(req models.RicartReply, reply *models.RicartReply) error {
	c.ra.HandleReply()
	return nil
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run client.go <client_id> <client_port> <server_address>")
		fmt.Println("Example: go run client_app/client.go 0 9000 localhost:8080")
		return
	}

	var clientID int
	fmt.Sscanf(os.Args[1], "%d", &clientID)
	clientPort := os.Args[2]
	serverAddr := os.Args[3]

	// Peer nodes for client coordination (only clients participate in RA)
	clientNodes := map[int]string{
		0: "10.38.21.222:9000",
		1: "10.38.21.68:9001",
	}

	lc := &algorithms.LamportClock{Time: 0}

	ra := &algorithms.RicartAgrawala{
		ID:          clientID,
		Nodes:       clientNodes,
		Clock:       lc,
		ServiceName: "ClientRA",
	}

	clientRA := &ClientRA{ra: ra}
	rpc.Register(clientRA)

	// Start RPC server for client-to-client RA
	listener, err := net.Listen("tcp", ":"+clientPort)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", clientPort, err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err == nil {
				go rpc.ServeConn(conn)
			}
		}
	}()

	fmt.Printf("Client %d RA server running on port %s\n", clientID, clientPort)

	// Dial the HMS server (Leader)
	serverClient, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatal("Dialing server:", err)
	}

	for {
		fmt.Println("\n--- Hospital Management System ---")
		fmt.Println("1. Register Patient (Receptionist)")
		fmt.Println("2. Update Diagnosis (Doctor)")
		fmt.Println("3. Generate Bill (Nurse)")
		fmt.Println("4. Get Patient Record")
		fmt.Println("5. Exit")
		fmt.Print("Choose option: ")

		var choice int
		fmt.Scan(&choice)

		switch choice {
		case 1:
			var id int
			var name string
			fmt.Print("Enter Patient ID: ")
			fmt.Scan(&id)
			fmt.Print("Enter Patient Name: ")
			fmt.Scan(&name)

			ra.RequestCS()
			args := models.RegisterArgs{ID: id, Name: name}
			var reply models.GenericReply
			err = serverClient.Call("HMS.RegisterPatient", args, &reply)
			ra.ReleaseCS()

			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Printf("Result: %v, Message: %s\n", reply.Success, reply.Message)
			}
		case 2:
			var id int
			var diagnosis string
			fmt.Print("Enter Patient ID: ")
			fmt.Scan(&id)
			fmt.Print("Enter Diagnosis: ")
			fmt.Scan(&diagnosis)

			ra.RequestCS()
			args := models.DiagnosisArgs{ID: id, Diagnosis: diagnosis}
			var reply models.GenericReply
			err = serverClient.Call("HMS.UpdateDiagnosis", args, &reply)
			ra.ReleaseCS()

			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Printf("Result: %v, Message: %s\n", reply.Success, reply.Message)
			}
		case 3:
			var id int
			var amount float64
			fmt.Print("Enter Patient ID: ")
			fmt.Scan(&id)
			fmt.Print("Enter Bill Amount: ")
			fmt.Scan(&amount)

			ra.RequestCS()
			args := models.BillArgs{ID: id, Amount: amount}
			var reply models.GenericReply
			err = serverClient.Call("HMS.GenerateBill", args, &reply)
			ra.ReleaseCS()

			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Printf("Result: %v, Message: %s\n", reply.Success, reply.Message)
			}
		case 4:
			var id int
			fmt.Print("Enter Patient ID: ")
			fmt.Scan(&id)

			args := models.GetPatientArgs{ID: id}
			var reply models.GetPatientReply
			err = serverClient.Call("HMS.GetPatientRecord", args, &reply)
			if err != nil {
				fmt.Println("Error:", err)
			} else if reply.Success {
				p := reply.Patient
				fmt.Printf("Record: ID=%d, Name=%s, Diagnosis=%s, Bill=%.2f\n", p.ID, p.Name, p.Diagnosis, p.Bill)
			} else {
				fmt.Println("Patient not found.")
			}
		case 5:
			os.Exit(0)
		default:
			fmt.Println("Invalid choice.")
		}
	}
}
