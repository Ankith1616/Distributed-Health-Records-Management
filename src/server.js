const express = require("express");
const http = require("http");
const {
    Server
} = require("socket.io");
const path = require("path");

const ClusterManager = require("./cluster/ClusterManager");
const configureRoutes = require("./routes/index");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Configure Static Web Client
app.use(express.static(path.join(__dirname, "../public")));

app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "../public/dashboard.html"));
});

/* ---------------- System Initialization ---------------- */
const clusterManager = new ClusterManager(io);

/* ---------------- Socket Connections ---------------- */
let dashboardCount = 0;
let clientCount = 0;
let activeConnections = 0;

io.on("connection", (socket) => {
    activeConnections++;

    // Determine Role
    const role = socket.handshake.query.role || "Client";

    if (role === "Dashboard") {
        dashboardCount++;
        socket.customName = `Dashboard-${dashboardCount}`;
    } else {
        clientCount++;
        socket.customName = `Client-${clientCount}`;
    }

    clusterManager.log(`[CONNECTED] ${socket.customName}`);

    io.emit("clientCount", activeConnections);
    socket.emit("update", clusterManager.getState());

    socket.on("disconnect", () => {
        activeConnections--;
        io.emit("clientCount", activeConnections);
        clusterManager.log(`[DISCONNECTED] ${socket.customName}`);
    });
});

/* ---------------- HTTP Routes ---------------- */
app.use("/", configureRoutes(clusterManager, io));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
    clusterManager.log(`👑 Initial Cluster Leader: ${clusterManager.getState().leader}`);
});