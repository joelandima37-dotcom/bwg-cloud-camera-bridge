
require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const PORT = Number(process.env.PORT || 8766);
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_ANON_KEY in .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", service: "BWG Cloud Camera Bridge" }));
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocket.Server({
  server,
  maxPayload: 64 * 1024
});

// Church and session separation prevents signalling between unrelated churches.
const rooms = new Map();

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function roomKey(churchId, sessionId) {
  return `${churchId}:${sessionId}`;
}

function getRoom(churchId, sessionId) {
  const key = roomKey(churchId, sessionId);
  if (!rooms.has(key)) rooms.set(key, new Map());
  return rooms.get(key);
}

async function authenticate(token, churchId) {
  const { data, error } = await supabase.auth.getUser(token);

  if (error || !data.user) {
    throw new Error("Authentication failed. Please sign in again.");
  }

  const user = data.user;

  // The user must have an active role assigned to this organization.
  const { data: roles, error: roleError } = await supabase
    .from("sda_organization_roles")
    .select("role_code,status")
    .eq("organization_id", churchId)
    .eq("user_id", user.id)
    .eq("status", "active")
    .limit(1);

  if (roleError || !roles || roles.length === 0) {
    throw new Error("You do not have access to this church.");
  }

  return user;
}

function removeConnection(ws) {
  if (!ws.registration) return;

  const { churchId, sessionId, connectionId } = ws.registration;
  const room = rooms.get(roomKey(churchId, sessionId));

  if (!room) return;

  room.delete(connectionId);

  for (const peer of room.values()) {
    send(peer, {
      type: "peer-disconnected",
      connectionId
    });
  }

  if (room.size === 0) {
    rooms.delete(roomKey(churchId, sessionId));
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", async (raw) => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "Invalid JSON message." });
    }

    try {
      if (message.type === "register") {
        if (ws.registration) {
          throw new Error("This connection is already registered.");
        }

        const token = message.token;
        const churchId = message.churchId;
        const sessionId = message.sessionId;
        const connectionId = message.connectionId;
        const role = message.role;

        if (!token || !churchId || !sessionId || !connectionId) {
          throw new Error("Missing registration information.");
        }

        if (!["phone", "desktop"].includes(role)) {
          throw new Error("Invalid connection role.");
        }

        if (
          typeof churchId !== "string" ||
          typeof sessionId !== "string" ||
          typeof connectionId !== "string" ||
          churchId.length > 100 ||
          sessionId.length > 100 ||
          connectionId.length > 100
        ) {
          throw new Error("Invalid registration values.");
        }

        const user = await authenticate(token, churchId);

        const room = getRoom(churchId, sessionId);

        // A connection ID must be unique within this church session.
        if (room.has(connectionId)) {
          throw new Error("That camera or production connection is already connected.");
        }

        ws.registration = {
          churchId,
          sessionId,
          connectionId,
          role,
          userId: user.id
        };

        room.set(connectionId, ws);

        send(ws, {
          type: "registered",
          connectionId,
          churchId,
          sessionId
        });

        for (const [id, peer] of room.entries()) {
          if (peer !== ws) {
            send(peer, {
              type: "peer-connected",
              connectionId,
              role
            });

            send(ws, {
              type: "peer-connected",
              connectionId: id,
              role: peer.registration.role
            });
          }
        }

        return;
      }

      if (!ws.registration) {
        throw new Error("Register and authenticate before sending messages.");
      }

      const { churchId, sessionId, connectionId } = ws.registration;
      const room = rooms.get(roomKey(churchId, sessionId));

      if (!room) {
        throw new Error("Church session is no longer available.");
      }

      if (message.type === "list-peers") {
        const peers = [];

        for (const [id, peer] of room.entries()) {
          if (peer !== ws) {
            peers.push({
              connectionId: id,
              role: peer.registration.role
            });
          }
        }

        return send(ws, { type: "peer-list", peers });
      }

      const allowedTypes = [
        "offer",
        "answer",
        "ice-candidate",
        "disconnect-camera"
      ];

      if (!allowedTypes.includes(message.type)) {
        throw new Error("Unsupported signalling message.");
      }

      const targetId = message.targetId;

      if (!targetId || typeof targetId !== "string" || targetId.length > 100) {
        throw new Error("A valid targetId is required.");
      }

      const target = room.get(targetId);

      if (!target || target.readyState !== WebSocket.OPEN) {
        throw new Error("The selected peer is not connected.");
      }

      // Forward only within the same church and session.
      send(target, {
        type: message.type,
        fromId: connectionId,
        data: message.data || null
      });
    } catch (error) {
      send(ws, {
        type: "error",
        message: error.message || "Request failed."
      });
    }
  });

  ws.on("close", () => removeConnection(ws));
  ws.on("error", () => removeConnection(ws));
});

// Remove dead connections.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

wss.on("close", () => clearInterval(heartbeat));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`BWG Cloud Camera Bridge listening on port ${PORT}`);
});
