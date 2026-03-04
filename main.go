// Distributed Real-Time Patient Health Record Management System
// ─────────────────────────────────────────────────────────────
// Inspired by Google BigTable / Apache HBase
//
// Architecture:
//
//	ZooKeeper  — leader election + heartbeat monitoring
//	Master     — REST API + lock table + consensus orchestration
//	RS-1/RS-2  — data nodes (store, vote, replicate)
//	Dashboard  — WebSocket live UI on :9000
//	Clients    — 5 concurrent goroutines sending random requests
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distributed-health-system/client"
	"distributed-health-system/dashboard"
	"distributed-health-system/master"
	"distributed-health-system/models"
	"distributed-health-system/regionserver"
	"distributed-health-system/utils"
	"distributed-health-system/zookeeper"
)

func main() {
	// ─── 0. Configuration Flags ──────────────────────────────────────
	role := flag.String("role", "ALL", "Role of this node (ALL, MASTER, RS, CLIENT, DASHBOARD)")
	nodeID := flag.String("id", "", "Unique ID for this node (required for MASTER and RS)")
	addr := flag.String("addr", "localhost", "IP/Hostname this node should advertise (e.g. 192.168.1.5)")
	masterAddr := flag.String("master", "localhost:8000", "Address of the primary Master node")
	clientCount := flag.Int("clients", 5, "Number of concurrent clients to start (CLIENT mode only)")

	flag.Parse()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║   Distributed Health Record Management System        ║")
	fmt.Println("║   Role: " + strings.ToUpper(*role) + "                                       ")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	// ─── 1. ZooKeeper (Always needed for coordination) ─────────────
	// In a real multi-system setup, ZK would be at a fixed IP.
	// For this simulation, we'll assume the Master-1 PC hosts ZK.
	zk := zookeeper.New()
	zk.Start()

	// ─── 2. Execution based on Role ──────────────────────────────────
	switch strings.ToUpper(*role) {
	case "ALL":
		runAllInOne(zk, *addr, *clientCount)
	case "MASTER":
		runMasterNode(zk, *nodeID, *addr)
	case "RS", "REGIONSERVER":
		runRegionServerNode(zk, *nodeID, *addr)
	case "CLIENT":
		runClientSimulator(*masterAddr, *clientCount)
	case "DASHBOARD":
		runDashboardOnly(zk, *addr)
	default:
		fmt.Printf("❌ Unknown role: %s. Use -role=MASTER|RS|CLIENT|DASHBOARD|ALL\n", *role)
		os.Exit(1)
	}

	// Block until OS interrupt (Ctrl+C).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	utils.GlobalLogger.Info("MAIN", "Shutting down gracefully…")
}

func runAllInOne(zk *zookeeper.ZooKeeper, host string, cCount int) {
	utils.GlobalLogger.Event("MAIN", "✅ Starting full cluster on local machine")

	m1Addr := host + ":8000"
	m2Addr := host + ":8003"
	rs1Addr := host + ":8001"
	rs2Addr := host + ":8002"

	// ─── 2. RegionServers ─────────────────────────────────────────────
	// Two RegionServer nodes (simulating HDFS data nodes).
	// - Each stores patient records in its local in-memory DataStore
	// - Each participates in consensus voting (YES/NO)
	// - Replication factor = 2: Master writes primary + secondary
	rs1 := regionserver.New("RS-1", rs1Addr, zk)
	rs2 := regionserver.New("RS-2", rs2Addr, zk)
	rs1.Start()
	rs2.Start()
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ RegionServers online: RS-1(%s), RS-2(%s)", rs1Addr, rs2Addr))

	// ─── 3. Master Nodes ──────────────────────────────────────────────
	// Two Master nodes for high availability simulation.
	// - Receives all client requests (create/read/update)
	// - Enforces row-level locks (mutual exclusion)
	// - Runs consensus before committing writes
	// - Routes writes to primary RS, replicates to secondary
	m1 := master.New("MASTER-1", m1Addr, zk)
	m1.AddRegionServer("RS-1", rs1Addr)
	m1.AddRegionServer("RS-2", rs2Addr)
	m1.Start()

	m2 := master.New("MASTER-2", m2Addr, zk)
	m2.AddRegionServer("RS-1", rs1Addr)
	m2.AddRegionServer("RS-2", rs2Addr)
	m2.Start()

	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ Masters online: MASTER-1(%s), MASTER-2(%s)", m1Addr, m2Addr))

	// Wait for all servers to fully bind before clients start.
	time.Sleep(800 * time.Millisecond)

	// ─── 4. Dashboard ─────────────────────────────────────────────────
	// Real-time web dashboard on :9000
	// - Serves dashboard.html (embedded at compile time)
	// - Upgrades /ws to WebSocket for live state broadcasts
	hub := startDashboard(zk)
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ Dashboard online: http://%s:9000", host))

	// ─── 5. Client Simulator ──────────────────────────────────────────
	// 5 concurrent goroutines simulate hospital staff.
	// Each randomly performs: CREATE / READ / UPDATE
	// Demonstrates:
	//   • Mutual exclusion: two clients updating same patient → lock + wait queue
	//   • Parallel reads: allowed simultaneously (no lock needed)
	//   • Consensus: every write triggers a majority vote
	sim := client.New(m1Addr, m2Addr)
	sim.Start(cCount)
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ %d client simulators started", cCount))

	// ─── 6. State Collector ───────────────────────────────────────────
	// Every 2 seconds, collect a SystemState snapshot and broadcast
	// it to all connected dashboard browsers via WebSocket.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			state := collectState(zk, m1, m2, sim)
			hub.BroadcastState(state)
		}
	}()

	printSummary(host)
}

// ─── Helpers ──────────────────────────────────────────────────────

// getAddr ensures a host has a port. If it already has one, it returns it as is.
// If it doesn't, it appends the defaultPort.
func getAddr(host, defaultPort string) string {
	if strings.Contains(host, ":") {
		return host
	}
	return host + ":" + defaultPort
}

func runMasterNode(zk *zookeeper.ZooKeeper, id, host string) {
	if id == "" {
		id = "MASTER-1"
	}
	mAddr := getAddr(host, "8000")
	m := master.New(id, mAddr, zk)
	// RS info would typically come from ZK discovery in real life...
	m.AddRegionServer("RS-1", getAddr(host, "8001"))
	m.AddRegionServer("RS-2", getAddr(host, "8002"))
	m.Start()
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ Master %s online at %s", id, mAddr))
}

func runRegionServerNode(zk *zookeeper.ZooKeeper, id, host string) {
	if id == "" {
		id = "RS-" + fmt.Sprintf("%d", time.Now().Unix()%100)
	}
	rsAddr := getAddr(host, "8001")
	rs := regionserver.New(id, rsAddr, zk)
	rs.Start()
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ RegionServer %s online at %s", id, rsAddr))
}

func runClientSimulator(mAddrs string, count int) {
	addrs := strings.Split(mAddrs, ",")
	sim := client.New(addrs...)
	sim.Start(count)
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ %d client simulators targeting %s", count, mAddrs))
}

func runDashboardOnly(zk *zookeeper.ZooKeeper, host string) {
	startDashboard(zk)
	utils.GlobalLogger.Event("MAIN", fmt.Sprintf("✅ Dashboard online at http://%s:9000", host))
	// In a dashboard-only mode, we don't have direct access to master/client instances
	// to collect state. This would require a more complex setup where the dashboard
	// actively queries masters for state, or masters push state to the dashboard.
	// For this simulation, the dashboard will just be a passive listener.
}

func startDashboard(zk *zookeeper.ZooKeeper) *dashboard.Hub {
	hub := dashboard.NewHub()
	dashMux := http.NewServeMux()
	dashMux.HandleFunc("/", dashboard.Handler)
	dashMux.HandleFunc("/ws", hub.WSHandler)

	go func() {
		srv := &http.Server{Addr: ":9000", Handler: dashMux}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.GlobalLogger.Error("MAIN", fmt.Sprintf("Dashboard error: %v", err))
		}
	}()
	return hub
}

func printSummary(host string) {
	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  🌐 Dashboard:      http://" + host + ":9000")
	fmt.Println("  🏠 Master-1 API:   http://" + host + ":8000 (Primary)")
	fmt.Println("  🏠 Master-2 API:   http://" + host + ":8003 (Secondary)")
	fmt.Println("  🖥️  RegionServer-1:  http://" + host + ":8001")
	fmt.Println("  🖥️  RegionServer-2:  http://" + host + ":8002")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Press Ctrl+C to stop")
	fmt.Println()
}

// collectState builds a full SystemState snapshot for the dashboard.
// This is called every 2 seconds by the state collector goroutine.
func collectState(
	zk *zookeeper.ZooKeeper,
	m1 *master.MasterNode,
	m2 *master.MasterNode,
	sim *client.Simulator,
) *models.SystemState {
	// Pick the active leader master for state collection.
	leader := zk.GetLeader()
	var activeMaster *master.MasterNode = m1
	if leader != nil && leader.ID == m2.ID {
		activeMaster = m2
	}

	return &models.SystemState{
		Timestamp:      time.Now(),
		Leader:         leader,
		Nodes:          zk.GetNodes(),
		Clients:        sim.GetClientStates(),
		LockTable:      activeMaster.GetLockSnapshot(),
		LastConsensus:  activeMaster.GetLastConsensus(),
		AllConsensus:   activeMaster.GetConsensusHistory(),
		PatientRecords: activeMaster.GetAllRecords(),
		Logs:           utils.GlobalLogger.GetRecent(100),
		RequestHistory: activeMaster.GetRequestHistory(),
		FailureEvents:  zk.GetFailureEvents(),
	}
}
