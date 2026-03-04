# Distributed Patient Health Record System

A highly available, fault-tolerant distributed system for managing patient health records. Built with Go, this system demonstrates key distributed systems concepts including leader election, consensus, mutual exclusion, and data replication.

## 🏗️ Architecture

The system follows a master-worker architecture:

- **Masters**: Central coordinators (High Availability via Leader Election).
- **RegionServers**: Data nodes (Sharded and Replicated storage).
- **ZooKeeper (Simulated)**: Coordination service for heartbeats and election.
- **Clients**: Simulated hospital staff performing concurrent CRUD operations.
- **Dashboard**: Real-time visualization of the cluster state.

### Key Concepts Implemented:
- **Leader Election**: Uses a lowest-ID algorithm with ZooKeeper heartbeats to ensure only one Master is active.
- **Mutual Exclusion**: Row-level locks prevent concurrent writes to the same patient record.
- **Consensus**: Majority voting (3-node minimum) for every data modification.
- **Data Replication**: HDFS-inspired replication (Factor=2) ensures data survives single-node failure.
- **Persistence**: RegionServers flush data to disk (`rs_data_*.json`) to recover state after a crash.

---

## 🚀 Running the System

### Option 1: Quick Start (All-in-One)
Run everything on a single machine:
```bash
go run main.go -role=all
```
Then visit: `http://localhost:9000`

### Option 2: Distributed (Multiple Machines)
To run across different PCs, follow these steps:

#### 1. Start the Master (on PC A)
```bash
go run main.go -role=master -id=MASTER-1 -addr=IP_OF_PC_A:8000
```

#### 2. Start RegionServers (on PC B, C, etc.)
```bash
go run main.go -role=regionserver -id=RS-1 -addr=IP_OF_PC_B:8001 -master=IP_OF_PC_A:8000
```

#### 3. Start Clients (on any PC)
```bash
go run main.go -role=client -clients=5 -master=IP_OF_PC_A:8000
```

#### 4. Start Dashboard (on any PC)
```bash
go run main.go -role=dashboard -master=IP_OF_PC_A:8000
```

---

## 🧪 Automated Verification

Run the integration test suite to verify distributed properties:
```bash
go test ./tests/...
```

### Tests Covered:
- **Mutual Exclusion**: Concurrent clients updating the same record.
- **Leader Election**: Automated failover when the primary Master is killed.
- **Data Persistence**: Recovery of data after RegionServer restart.

## 📊 Monitoring
The built-in dashboard provides:
- Live Cluster Topology
- Node Heartbeats & Status
- Distributed Lock Table
- Consensus History
- Global Patient Registry
- Real-time System Logs
