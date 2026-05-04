"""
WebSocket Test App — FastNest Backend
======================================
Minimal app to test WebSocket features.

Run:
    pip install fastnest uvicorn
    uvicorn ws_test:app --reload --port 8000
"""

import time
import asyncio
import json

from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

from fastnest.core.decorators  import Module, Injectable
from fastnest.core.factory      import create_app
from fastnest.core.websocket    import (
    WebSocketGateway, SubscribeMessage,
    OnConnect, OnDisconnect, WebSocketClient,
)


# ── Service: tracks rooms and stats ──────────────────────────────
@Injectable()
class ChatService:
    def __init__(self):
        # room_name -> list of clients
        self._rooms: dict[str, list[WebSocketClient]] = {}
        self._msg_count = 0

    def join(self, room: str, client: WebSocketClient):
        self._rooms.setdefault(room, [])
        if client not in self._rooms[room]:
            self._rooms[room].append(client)

    def leave(self, client: WebSocketClient):
        for members in self._rooms.values():
            if client in members:
                members.remove(client)

    def room_members(self, room: str) -> list[WebSocketClient]:
        return list(self._rooms.get(room, []))

    def total_clients(self) -> int:
        seen = set()
        for members in self._rooms.values():
            for c in members:
                seen.add(c.id)
        return len(seen)

    def rooms_summary(self) -> dict:
        return {r: len(m) for r, m in self._rooms.items() if m}

    def inc(self): self._msg_count += 1
    def msg_count(self): return self._msg_count

    async def broadcast_to_room(self, room: str, event: str, data):
        dead = []
        for client in self.room_members(room):
            try:
                await client.send({"event": event, "data": data})
            except Exception:
                dead.append(client)
        for d in dead:
            self._rooms[room].remove(d)


# ── Gateway ───────────────────────────────────────────────────────
@WebSocketGateway("/ws")
class TestGateway:
    def __init__(self, svc: ChatService):
        self.svc = svc

    @OnConnect()
    async def on_connect(self, client: WebSocketClient):
        # welcome message with current stats
        await client.send({
            "event": "connected",
            "data": {
                "client_id":     client.id,
                "total_clients": self.svc.total_clients(),
                "server_time":   time.time(),
            }
        })

    @OnDisconnect()
    async def on_disconnect(self, client: WebSocketClient):
        room = client.data.get("room")
        self.svc.leave(client)
        # notify room that someone left
        if room:
            await self.svc.broadcast_to_room(room, "user:left", {
                "client_id": client.id,
                "name":      client.data.get("name", "Anonymous"),
                "room":      room,
                "members":   len(self.svc.room_members(room)),
            })

    @SubscribeMessage("ping")
    async def on_ping(self, data, client: WebSocketClient):
        # simple round-trip latency test
        await client.send({
            "event": "pong",
            "data": {
                "client_ts":  data.get("ts") if isinstance(data, dict) else None,
                "server_ts":  time.time(),
                "client_id":  client.id,
            }
        })

    @SubscribeMessage("join")
    async def on_join(self, data, client: WebSocketClient):
        # data: {"room": "general", "name": "Alice"}
        if isinstance(data, str):
            data = {"room": data}
        room = data.get("room", "general")
        name = data.get("name", f"User-{client.id[:6]}")

        client.data["room"] = room
        client.data["name"] = name
        self.svc.join(room, client)

        # confirm to joiner
        await client.send({
            "event": "joined",
            "data": {
                "room":    room,
                "name":    name,
                "members": len(self.svc.room_members(room)),
            }
        })
        # notify others in room
        await self.svc.broadcast_to_room(room, "user:joined", {
            "client_id": client.id,
            "name":      name,
            "room":      room,
            "members":   len(self.svc.room_members(room)),
        })

    @SubscribeMessage("message")
    async def on_message(self, data, client: WebSocketClient):
        # broadcast to everyone in the same room
        self.svc.inc()
        room = client.data.get("room", "general")
        name = client.data.get("name", "Anonymous")
        payload = {
            "client_id":   client.id,
            "name":        name,
            "room":        room,
            "text":        str(data) if not isinstance(data, dict) else data.get("text", ""),
            "ts":          time.time(),
            "total_msgs":  self.svc.msg_count(),
        }
        await self.svc.broadcast_to_room(room, "message", payload)

    @SubscribeMessage("stats")
    async def on_stats(self, data, client: WebSocketClient):
        # return server-side stats
        await client.send({
            "event": "stats",
            "data": {
                "total_clients": self.svc.total_clients(),
                "total_msgs":    self.svc.msg_count(),
                "rooms":         self.svc.rooms_summary(),
                "server_time":   time.time(),
            }
        })

    @SubscribeMessage("broadcast:all")
    async def on_broadcast(self, data, client: WebSocketClient):
        # broadcast to ALL rooms (admin-style)
        name = client.data.get("name", "Anonymous")
        text = str(data) if not isinstance(data, dict) else data.get("text", "")
        for room in list(self.svc.rooms_summary().keys()):
            await self.svc.broadcast_to_room(room, "announcement", {
                "from":   name,
                "text":   text,
                "ts":     time.time(),
            })
        await client.send({"event": "sent", "data": "Broadcast delivered"})

    @SubscribeMessage("timer")
    async def on_timer(self, data, client: WebSocketClient):
        # send 5 updates every 1 second (demonstrates server push)
        secs = int(data) if isinstance(data, (int, str)) and str(data).isdigit() else 5
        secs = min(secs, 10)
        for i in range(1, secs + 1):
            await asyncio.sleep(1)
            try:
                await client.send({
                    "event": "timer:tick",
                    "data": {"tick": i, "total": secs, "ts": time.time()}
                })
            except Exception:
                break
        try:
            await client.send({"event": "timer:done", "data": {"ticks": secs}})
        except Exception:
            pass


@Module(gateways=[TestGateway], providers=[ChatService])
class AppModule:
    pass


app = create_app(AppModule, debug=True, title="WS Test")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
