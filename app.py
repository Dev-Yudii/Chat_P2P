"""
app.py — Servidor web P2P Chat
Roda interface web local via Flask + Socket.IO
"""

import sys
import os
import json
import threading
import logging
import argparse

# Adiciona o diretório atual ao path para imports relativos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

from network.peer_manager import PeerManager
from models.message import Message, MSG_CHAT, MSG_JOIN, MSG_LEAVE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("app")

app = Flask(__name__)
app.config["SECRET_KEY"] = "p2p-chat-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

manager: PeerManager = None


# ─────────────────────────────── Callbacks ──────────────────────────────────

def on_message(msg: Message):
    """Chamado quando chega mensagem de outro peer — repassa ao browser via WS."""
    if msg.type == MSG_CHAT:
        socketio.emit("chat_message", {
            "sender": msg.sender,
            "content": msg.content,
            "timestamp": msg.timestamp,
            "id": msg.id,
        })
    elif msg.type == MSG_JOIN:
        socketio.emit("system_event", {
            "text": f"🟢 {msg.sender} entrou na sala",
            "type": "join"
        })
        socketio.emit("network_update", manager.get_status())
    elif msg.type == MSG_LEAVE:
        socketio.emit("system_event", {
            "text": f"🔴 {msg.sender} saiu da sala",
            "type": "leave"
        })
        socketio.emit("network_update", manager.get_status())


def on_network_change():
    """Chamado quando a topologia da rede muda — atualiza o painel lateral."""
    if manager:
        socketio.emit("network_update", manager.get_status())


# ──────────────────────────────── Rotas HTTP ────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", port=manager.port, name=manager.name)


@app.route("/api/status")
def api_status():
    return jsonify(manager.get_status())


@app.route("/api/history")
def api_history():
    msgs = manager.history.get_last(100)
    return jsonify(msgs)


@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.get_json()
    host = data.get("host", "127.0.0.1")
    port = int(data.get("port"))
    ok = manager.connect_to_peer(host, port)
    return jsonify({"success": ok, "status": manager.get_status()})


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    manager.stop()
    threading.Timer(0.5, lambda: os._exit(0)).start()
    return jsonify({"success": True})


# ──────────────────────────── Socket.IO Events ──────────────────────────────

@socketio.on("connect")
def on_ws_connect():
    emit("network_update", manager.get_status())
    # Envia histórico recente ao conectar
    msgs = manager.history.get_last(50)
    emit("history", msgs)


@socketio.on("send_message")
def on_send_message(data):
    content = data.get("content", "").strip()
    if not content:
        return
    msg = Message(sender=manager.name, content=content, msg_type=MSG_CHAT)
    manager.history.save(msg)
    manager.broadcast(msg)
    # Ecoa para o próprio cliente
    emit("chat_message", {
        "sender": manager.name,
        "content": msg.content,
        "timestamp": msg.timestamp,
        "id": msg.id,
        "own": True,
    })


@socketio.on("connect_to_peer")
def on_ws_connect_peer(data):
    host = data.get("host", "127.0.0.1")
    port = int(data.get("port"))
    ok = manager.connect_to_peer(host, port, announce_join=True)
    # Get the peer name if connected
    peer_name = f"peer_{port}"
    if ok:
        status = manager.get_status()
        for p in status.get("peers", []):
            if p["port"] == port:
                peer_name = p["name"]
                break
    emit("connect_result", {"success": ok, "port": port, "name": peer_name})
    emit("network_update", manager.get_status())


@socketio.on("request_status")
def on_request_status():
    emit("network_update", manager.get_status())


# ───────────────────────────────── Main ─────────────────────────────────────

def main():
    global manager

    parser = argparse.ArgumentParser(description="P2P Chat Web")
    parser.add_argument("port", type=int, nargs="?", default=5001,
                        help="Porta TCP do peer (default: 5001)")
    parser.add_argument("--name", type=str, default=None,
                        help="Nome/apelido do peer")
    parser.add_argument("--web-port", type=int, default=None,
                        help="Porta da interface web (default: porta_tcp + 1000)")
    args = parser.parse_args()

    tcp_port = args.port
    web_port = args.web_port or (tcp_port + 1000)
    nickname = args.name or f"peer_{tcp_port}"

    manager = PeerManager(
        host="0.0.0.0",
        port=tcp_port,
        on_message_cb=on_message,
        on_network_change_cb=on_network_change,
        nickname=nickname,
    )
    manager.start()

    log.info(f"")
    log.info(f"  ╔══════════════════════════════════════════╗")
    log.info(f"  ║       P2P CHAT — Interface Web           ║")
    log.info(f"  ╠══════════════════════════════════════════╣")
    log.info(f"  ║  Peer TCP  : porta {tcp_port:<24}║")
    log.info(f"  ║  Nome      : {nickname:<28}║")
    log.info(f"  ║  Web UI    : http://localhost:{web_port:<13}║")
    log.info(f"  ╚══════════════════════════════════════════╝")
    log.info(f"")

    try:
        socketio.run(app, host="0.0.0.0", port=web_port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        manager.stop()


if __name__ == "__main__":
    main()
