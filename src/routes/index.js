const express = require("express");
const router = express.Router();

module.exports = function(clusterManager, io) {

    // Standard client update route
    router.get("/update/:patientId/:client", async (req, res) => {
        const {
            patientId,
            client
        } = req.params;
        const result = await clusterManager.processUpdate(patientId, client);
        io.emit("update", clusterManager.getState());

        if (result.success) {
            res.status(200).send(result.message);
        } else {
            res.status(409).send(result.message);
        }
    });

    // Stress testing route mimicking N concurrent non-blocking requests hitting the endpoint simultaneously
    router.get("/stress/:patientId/:count", async (req, res) => {
        const {
            patientId
        } = req.params;
        const count = parseInt(req.params.count, 10);

        clusterManager.log(`🚀 STRESS TEST STARTED: Simulating ${count} concurrent clients targeting Patient [${patientId}]`);

        const tasks = [];
        for (let i = 1; i <= count; i++) {
            const clientName = `StressBot-${i}`;
            // Intentionally resolving asynchronously in parallel to attack mutual exclusion mechanics!
            tasks.push(clusterManager.processUpdate(patientId, clientName));
        }

        // Wait for all queue lines to finish processing completely
        const results = await Promise.all(tasks);

        clusterManager.log(`✅ STRESS TEST CONCLUDED for Patient [${patientId}]`);
        io.emit("update", clusterManager.getState());

        res.status(200).send({
            message: `Stress test finished for ${count} concurrent clients.`,
            results
        });
    });

    router.get("/failLeader", (req, res) => {
        const currentLeader = clusterManager.leaderElection.getLeader();
        // Specifically attack the CURRENT active leader forcing the auto heartbeat to flag it
        clusterManager.leaderElection.simulateFailure(currentLeader);
        res.status(200).send(`Manual failure injected for current leader.`);
    });

    router.get("/ping/:master", (req, res) => {
        // Mock recovering a master node by pinging its heartbeat back alive
        clusterManager.leaderElection.pingHeartbeat(req.params.master);
        res.status(200).send(`Pinged ${req.params.master}`);
    });

    router.get("/failRegion/:name", (req, res) => {
        clusterManager.failRegion(req.params.name);
        io.emit("update", clusterManager.getState());
        res.status(200).send(`Failed Region: ${req.params.name}`);
    });

    router.get("/recoverRegion/:name", (req, res) => {
        clusterManager.recoverRegion(req.params.name);
        io.emit("update", clusterManager.getState());
        res.status(200).send(`Recovered Region: ${req.params.name}`);
    });

    return router;
};