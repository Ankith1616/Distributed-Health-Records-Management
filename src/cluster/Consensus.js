/**
 * Consensus Module
 * Determines if a transaction can safely commit based on majority rules from only ONLINE servers.
 */
class Consensus {

    /**
     * Attempts a Distributed Two-Phase Prepare round predicting consensus success.
     * @param {Array} servers - List of total RegionServers in the cluster.
     * @param {Object} clusterSystem - Back-reference to orchestrator for log streaming.
     * @returns {Object} Data payload summarizing the final rule result and node feedback map.
     */
    majorityVote(servers, clusterSystem) {
        let positiveVotes = 0;
        const results = [];

        // Filter out OFFLINE nodes entirely to compute dynamic majority constraints on ONLINE metrics
        const onlineServers = servers.filter(s => s.isOnline);
        const totalOnline = onlineServers.length;

        // In a true fault-tolerant model, you must have > 0 online nodes entirely to gauge a system quorum
        if (totalOnline === 0) {
            clusterSystem.log('🚨 CONSENSUS SYSTEM HALTED: 0 / 0 servers available.');
            return {
                hasMajority: false,
                results: [],
                votes: 0,
                totalOnline: 0
            };
        }

        // Collect prepare votes from ACTIVE/ONLINE servers only
        onlineServers.forEach(server => {
            if (typeof server.prepare === 'function') {
                const response = server.prepare();

                // Track detailed decision map
                results.push({
                    server: server.getName(),
                    ...response
                });

                if (response.vote === true) {
                    positiveVotes++;
                }
            }
        });

        // Strict Majority Calculation on strictly available nodes (e.g. > 50%)
        // E.g. 2/3 = Passes | 2/2 = Passes | 1/2 = Fails | 1/1 = Passes
        const hasMajority = positiveVotes > Math.floor(totalOnline / 2);

        return {
            hasMajority,
            results,
            votes: positiveVotes,
            totalOnline
        };
    }
}

module.exports = Consensus;