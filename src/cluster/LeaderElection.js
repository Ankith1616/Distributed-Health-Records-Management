/**
 * LeaderElection Module (ZooKeeper Style)
 * Handles auto-failover, heartbeat monitoring, and highest-priority elections.
 */
class LeaderElection {
    constructor(clusterSystem) {
        this.clusterSystem = clusterSystem;
        // Priority map mapping candidate ID to their heartbeat status
        this.candidates = new Map();
        this.currentLeader = null;

        // Configuration
        this.heartbeatIntervalMs = 2000;
        this.timeoutMs = 5000;
        this.monitoringTimer = null;
    }

    /**
     * Register a candidate node with a given priority (Higher number = Higher Priority)
     */
    register(name, priority) {
        this.candidates.set(name, {
            priority: priority,
            lastHeartbeat: Date.now(),
            isOnline: true
        });

        if (!this.currentLeader) {
            this.electLeader();
        }
    }

    /**
     * Start the continuous monitoring loop
     */
    startMonitoring() {
        if (this.monitoringTimer) clearInterval(this.monitoringTimer);

        this.monitoringTimer = setInterval(() => {
            this.checkHeartbeats();
        }, this.heartbeatIntervalMs);
    }

    /**
     * Simulate a candidate node sending a heartbeat ping
     */
    pingHeartbeat(name) {
        if (this.candidates.has(name)) {
            const node = this.candidates.get(name);
            node.lastHeartbeat = Date.now();

            // If it was dead and is now recovered, we might need an election
            if (!node.isOnline) {
                node.isOnline = true;
                this.clusterSystem.log(`💓 Master Node [${name}] recovered heartbeat.`);
                this.electLeader();
            }
        }
    }

    /**
     * Iterate over candidate heartbeats and detect failures
     */
    checkHeartbeats() {
        const now = Date.now();
        let leaderFailed = false;

        for (const [name, data] of this.candidates.entries()) {
            if (data.isOnline && (now - data.lastHeartbeat) > this.timeoutMs) {
                data.isOnline = false;
                this.clusterSystem.log(`💔 Heartbeat LOST for Master Node [${name}]! Marking Offline.`);

                if (this.currentLeader === name) {
                    leaderFailed = true;
                }
            }
        }

        if (leaderFailed) {
            this.clusterSystem.log(`⚠️ CURRENT LEADER ${this.currentLeader} FAILED. Forcing Re-Election!`);
            this.electLeader();
        }
    }

    /**
     * Fail a specific node manually (for simulation)
     */
    simulateFailure(name) {
        const node = this.candidates.get(name);
        if (node && node.isOnline) {
            node.isOnline = false;
            // artificially backdate heartbeat to force fail
            node.lastHeartbeat = Date.now() - (this.timeoutMs * 2);
            this.checkHeartbeats();
        }
    }

    /**
     * Elects the highest priority ONLINE node to prevent split-brain logic
     */
    electLeader() {
        let highestPriority = -1;
        let elected = null;

        for (const [name, data] of this.candidates.entries()) {
            if (data.isOnline && data.priority > highestPriority) {
                highestPriority = data.priority;
                elected = name;
            }
        }

        if (elected) {
            if (this.currentLeader !== elected) {
                this.currentLeader = elected;
                this.clusterSystem.log(`👑 New Leader Elected: ${this.currentLeader} (Priority: ${highestPriority})`);
                this._broadcast();
            }
        } else {
            this.currentLeader = "NO_LEADER_AVAILABLE";
            this.clusterSystem.log(`❌ CRITICAL ALERT: All Master Nodes Offline. Cluster degraded.`);
            this._broadcast();
        }

        return this.currentLeader;
    }

    getLeader() {
        return this.currentLeader;
    }

    _broadcast() {
        if (this.clusterSystem && this.clusterSystem.io) {
            this.clusterSystem.io.emit("update", this.clusterSystem.getState());
        }
    }
}

module.exports = LeaderElection;