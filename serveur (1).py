# serveur.py
import asyncio
import json
import uuid
import ssl
import socket
from pathlib import Path
from aiohttp import web
import aiohttp

rooms = {}

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    peer_id = str(uuid.uuid4())[:8]
    room_id = request.query.get("room", "salle1")
    peer_name = request.query.get("name", f"Participant-{peer_id[:4]}")

    if room_id not in rooms:
        rooms[room_id] = []

    peer = {"id": peer_id, "name": peer_name, "ws": ws}
    rooms[room_id].append(peer)

    print(f"[+] {peer_name} ({peer_id}) connecté — salle '{room_id}' ({len(rooms[room_id])}/20)")

    # Notifier les autres
    await broadcast(room_id, {
        "type": "user-joined",
        "id": peer_id,
        "name": peer_name,
        "count": len(rooms[room_id])
    }, exclude=peer_id)

    # Envoyer la liste des participants existants avec leurs noms
    existing = [
        {"id": p["id"], "name": p["name"]}
        for p in rooms[room_id] if p["id"] != peer_id
    ]
    await ws.send_json({
        "type": "existing-peers",
        "peers": existing,
        "your-id": peer_id,
        "your-name": peer_name
    })

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                data["from"] = peer_id
                data["from-name"] = peer_name
                target_id = data.get("to")
                if target_id:
                    await send_to(room_id, target_id, data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
    finally:
        rooms[room_id] = [p for p in rooms[room_id] if p["id"] != peer_id]
        print(f"[-] {peer_name} déconnecté — salle '{room_id}' ({len(rooms[room_id])}/20)")
        await broadcast(room_id, {
            "type": "user-left",
            "id": peer_id,
            "name": peer_name,
            "count": len(rooms[room_id])
        }, exclude=None)

    return ws


async def broadcast(room_id, message, exclude=None):
    if room_id not in rooms:
        return
    for peer in rooms[room_id]:
        if peer["id"] != exclude:
            try:
                await peer["ws"].send_json(message)
            except:
                pass


async def send_to(room_id, target_id, message):
    if room_id not in rooms:
        return
    for peer in rooms[room_id]:
        if peer["id"] == target_id:
            try:
                await peer["ws"].send_json(message)
            except:
                pass
            break


async def index_handler(request):
    raise web.HTTPFound("/static/index.html")


app = web.Application()
app.router.add_get("/", index_handler)
app.router.add_get("/ws", websocket_handler)

project_root = Path(__file__).resolve().parent
static_dir = project_root / "static"
if static_dir.exists() and static_dir.is_dir():
    app.router.add_static("/static", path=str(static_dir), name="static")
else:
    app.router.add_static("/static", path=str(project_root), name="static")
    print("Warning: no static/ directory found; serving files from the project root.")

if __name__ == "__main__":
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain("cert.pem", "key.pem")

    local_ip = "10.179.60.221"

    print(f"\n{'='*55}")
    print(f"  Serveur vidéoconférence HTTPS démarré !")
    print(f"{'='*55}")
    print(f"  Local  : https://localhost:8443")
    print(f"  Réseau : https://{local_ip}:8443")
    print(f"{'='*55}\n")

    web.run_app(app, host="0.0.0.0", port=8443, ssl_context=ssl_context, print=False)
