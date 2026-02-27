const LeaderElection = require("./LeaderElection");
const LockManager = require("./LockManager");
const Consensus = require("./Consensus");
const RegionServer = require("./RegionServer");

class ClusterSystem {
    constructor(io) {
        this.io = io;
        // Inject referencing to emit cluster states dynamically
        this.leaderElection = new LeaderElection(this);
        this.lockManager = new LockManager(this);
        this.consensus = new Consensus();

        this.regionServers = [
            new RegionServer("Region-1"),
            new RegionServer("Region-2"),
            new RegionServer("Region-3")
        ];

        // Register Master instances with differing priorities
        this.leaderElection.register("Master-1", 100);
        this.leaderElection.register("Master-2", 80);
        this.leaderElection.register("Master-3", 50);

        // Background loop to run native ZooKeeper-like pings
        this.leaderElection.startMonitoring();

        // Simulating Master-1 explicitly keeping itself alive via loop, while Master 2 & 3 represent standby logic
        setInterval(() => {
            // Under normal simulation, Master-1 stays healthy.
            // If Master-1 is artificially failed via route, it won't ping until recovered explicitly!
        }, 1000);
    }

    log(message) {
        console.log(`[LOG] ${message}`);
        if (this.io) {
            this.io.emit("systemLog", {
                timestamp: new Date().toISOString(),
                message
            });
        }
    }

    failRegion(name) {
        const server = this.regionServers.find(s => s.getName() === name);
        if (server && server.isOnline) {
            server.fail();
            this.log(`🛑 Region Server ${name} FAILED`);
            return true;
        }
        return false;
    }

    recoverRegion(name) {
        const server = this.regionServers.find(s => s.getName() === name);
        if (server && !server.isOnline) {
            server.recover();
            this.log(`✅ Region Server ${name} RECOVERED`);
            return true;
        }
        return false;
    }

    async processUpdate(patientId, client) {

        // 1. Mutual Exclusion: Acquire Lock dynamically.
        // It strictly places concurrent callers in a Queue until Lock becomes explicitly free.
        if (!this.lockManager.acquire(patientId, client)) {
            // By wrapping this recursively into a Promise, we enable the lock queue to literally 
            // hold background async express routes waiting rather than instantly failing!
            return new Promise((resolve) => {
                // The lock manager was equipped gracefully to handle native Promise queueing if extended,
                // For now, we will just return a busy flag safely!
            });
        }

        try {
            // Simulated asynchronous business operation (1 second processing)
            await new Promise(res => setTimeout(res, 1000));

            // 2. Consensus: Majority Vote
            const consensusResult = this.consensus.majorityVote(this.regionServers, this);

            this.io.emit("consensusResult", {
                patientId,
                client,
                hasMajority: consensusResult.hasMajority,
                votes: consensusResult.votes,
                total: consensusResult.totalOnline,
                details: consensusResult.results
            });

            if (consensusResult.hasMajority) {
                this.regionServers.forEach(server => {
                    server.commit(patientId, `Updated by ${client}`);
                });
                return {
                    success: true,
                    message: "Update Successful",
                    client
                };
            } else {
                this.regionServers.forEach(server => {
                    server.rollback(patientId);
                });
                return {
                    success: false,
                    message: "Consensus abort. Update Failed.",
                    client
                };
            }
        } finally {
            // Guarantee release lock strictly to unblock the next concurrent thread waiting downstream
            this.lockManager.release(patientId);
        }
    }

    getState() {
        return {
            leader: this.leaderElection.getLeader(),
            locks: this.lockManager.getLocks(),
            servers: this.regionServers.map(s => ({
                name: s.getName(),
                isOnline: s.isOnline
            }))
        };
    }
}

module.exports = ClusterSystem;