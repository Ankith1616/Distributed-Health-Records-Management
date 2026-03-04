package main

import (
	"fmt"
	"hms/models"
	"log"
	"net/rpc"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <server_address>")
		return
	}

	serverAddr := os.Args[1]
	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatal("Dialing:", err)
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

			args := models.RegisterArgs{ID: id, Name: name}
			var reply models.GenericReply
			err = client.Call("HMS.RegisterPatient", args, &reply)
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

			args := models.DiagnosisArgs{ID: id, Diagnosis: diagnosis}
			var reply models.GenericReply
			err = client.Call("HMS.UpdateDiagnosis", args, &reply)
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

			args := models.BillArgs{ID: id, Amount: amount}
			var reply models.GenericReply
			err = client.Call("HMS.GenerateBill", args, &reply)
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
			err = client.Call("HMS.GetPatientRecord", args, &reply)
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
