/**
 * RegionServer Module
 * Represents a simulated distributed database node participating securely with Consensus & Commits.
 */
class RegionServer {
    /**
     * Initializes the Region instance.
     * @param {string} name - Regional domain grouping mapping
     */
    constructor(name) {
        this.name = name;
        this.isOnline = true;
    }

    /**
     * Simulated fault toggler (Down)
     */
    fail() {
        this.isOnline = false;
    }

    /**
     * Simulated fault toggler (Up)
     */
    recover() {
        this.isOnline = true;
    }

    /**
     * Simulated Phase-1 (Prepare) logic. Occurs dynamically per consensus request.
     * Returns varying status contexts to map distributed random instability mimicking payload constraints.
     * @returns {Object} Phase 1 Result Map
     */
    prepare() {
        // Guard check just in case, though the Consensus module should filter it.
        if (!this.isOnline) {
            return {
                vote: false,
                reason: "OFFLINE"
            };
        }

        // Randomly simulate a failed local transaction validation (e.g. 15% chance of DB IO lock)
        const luck = Math.random();
        if (luck < 0.15) {
            return {
                vote: false,
                reason: "IO_ERROR (Simulated)"
            };
        } else if (luck < 0.25) {
            return {
                vote: false,
                reason: "CHECKSUM_FAILED (Simulated)"
            };
        }

        return {
            vote: true,
            reason: "ACK"
        };
    }

    /**
     * Phase-2 standard Commit. Executes only if System resolves Strict Quorum.
     * @param {string} id - Scoped patient record index
     * @param {string} data - Arbitrary context representation to store string
     */
    commit(id, data) {
        if (!this.isOnline) return;
        // Proceed with real data persistence mechanics -> file/in-memory update...
    }

    /**
     * Rollback procedure fired explicitly if the majority aborts.
     * This flushes uncommitted data from memory in simulated lock-pages.
     * @param {string} id - Scoped patient record index
     */
    rollback(id) {
        if (!this.isOnline) return;
        // Free memory buffers or Undo-Log allocations
        console.log(`[${this.name}] ♻️ Rolled-Back operations for ID ${id}`);
    }

    getName() {
        return this.name;
    }
}

module.exports = RegionServer;