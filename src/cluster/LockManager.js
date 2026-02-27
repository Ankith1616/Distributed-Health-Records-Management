/**
 * LockManager Module
 * Handles mutual exclusion with timeouts, dead-client prevention, and access queuing.
 */
class LockManager {
    constructor(clusterSystem, lockTimeoutMs = 15000) {
        // Map of recordId -> { owner: string, timestamp: number, timer: NodeJS.Timeout, queue: Array }
        this.locks = {};
        this.clusterSystem = clusterSystem;
        this.lockTimeoutMs = lockTimeoutMs;
    }

    /**
     * Attempts to acquire a lock dynamically.
     * @returns {boolean} True if successfully acquired; false if queued.
     */
    acquire(recordId, client) {
        if (this.locks[recordId] && this.locks[recordId].owner) {
            const lock = this.locks[recordId];

            if (!lock.queue.find(q => q.client === client)) {
                // For real concurrency, we push a raw callback array rather than just strings!
                // To keep it clean, the route layer checks the status itself for now.
                lock.queue.push({
                    client
                });
                this.clusterSystem.log(`🔒 Lock BUSY on Record ${recordId}. Client ${client} added to wait queue.`);
                this._broadcastState();
            }
            return false;
        }

        this._grantLock(recordId, client);
        return true;
    }

    _grantLock(recordId, client) {
        const timer = setTimeout(() => {
            this._handleTimeout(recordId);
        }, this.lockTimeoutMs);

        const existingQueue = this.locks[recordId] ? this.locks[recordId].queue : [];

        this.locks[recordId] = {
            owner: client,
            timestamp: Date.now(),
            timer: timer,
            queue: existingQueue
        };

        this.clusterSystem.log(`🔑 Lock ACQUIRED on Record ${recordId} by ${client}`);
        this._broadcastState();
    }

    _handleTimeout(recordId) {
        const lock = this.locks[recordId];
        if (lock) {
            this.clusterSystem.log(`⏱️ Lock TIMEOUT on Record ${recordId}`);
            this.release(recordId, true);
        }
    }

    release(recordId, isTimeout = false) {
        const lock = this.locks[recordId];
        if (lock) {
            clearTimeout(lock.timer);

            const oldOwner = lock.owner;
            const queue = lock.queue;

            delete this.locks[recordId];

            if (!isTimeout) {
                this.clusterSystem.log(`🔓 Lock RELEASED on Record ${recordId} (Was locked by ${oldOwner})`);
            }

            if (queue.length > 0) {
                const nextTarget = queue.shift();
                this.clusterSystem.log(`➡️ Lock AUTO-GRANTED to next in queue: ${nextTarget.client} for Record ${recordId}`);

                this.locks[recordId] = {
                    queue: queue
                };
                this._grantLock(recordId, nextTarget.client);

                // Note: Normally we would fulfill a Promise resolver here for true Async/Await yielding 
                // However, since we're using Express HTTP reqs under stress context, allowing the auto-grant 
                // internally resolves the logical lock chain effectively for visually confirming Dashboard queues.
                // Re-calling processUpdate() logically mimics resuming thread flow!
                this.clusterSystem.processUpdate(recordId, nextTarget.client);

            } else {
                this._broadcastState();
            }
        }
    }

    getLocks() {
        const cleanLocks = {};
        for (const [recordId, lock] of Object.entries(this.locks)) {
            if (lock.owner) {
                cleanLocks[recordId] = {
                    owner: lock.owner,
                    queueLen: lock.queue.length
                };
            }
        }
        return cleanLocks;
    }

    _broadcastState() {
        if (this.clusterSystem && this.clusterSystem.io) {
            this.clusterSystem.io.emit("update", this.clusterSystem.getState());
        }
    }
}

module.exports = LockManager;