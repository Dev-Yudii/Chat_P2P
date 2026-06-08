"""
PeerManager — Rede P2P totalmente descentralizada

Correções aplicadas:
1. Eleição de líder propagada via MSG_LEADER_ANNOUNCE para toda a rede
2. Gossip inclui o IP real do peer (não força 127.0.0.1)
3. Reconexão automática a todos os peers conhecidos ao perder um nó
4. Descoberta UDP broadcast na rede local (sem digitar porta manualmente)
5. Thread-safety melhorada em todos os locks
"""

import socket
import threading
import json
import time
import logging
from typing import Callable, Dict, Optional, List, Tuple

from models.message import (
    Message, MSG_CHAT, MSG_JOIN, MSG_LEAVE,
    MSG_PING, MSG_PONG, MSG_LEADER, MSG_HANDSHAKE, MSG_PEER_EX
)
from utils.history import ChatHistory

log = logging.getLogger("p2p")

# Porta UDP para descoberta na rede local
UDP_DISCOVERY_PORT = 47777

class PeerInfo:
    """Metadados de um peer conhecido (mesmo que desconectado temporariamente)."""
    def __init__(self, host: str, port: int, name: str = None):
        self.host = host
        self.port = port
        self.name = name or f"peer_{port}"
        self.sock: Optional[socket.socket] = None
        self.connected = False
        self.last_seen = time.time()
        self._announced_joins: set = set()


class PeerManager:
    def __init__(self, host: str, port: int,
                 on_message_cb: Callable[[Message], None],
                 on_network_change_cb: Callable[[], None] = None,
                 nickname: str = None):
        self.host = host
        self.port = port
        self.name = nickname or f"peer_{port}"
        self.on_message_external_cb = on_message_cb
        self.on_network_change_cb = on_network_change_cb or (lambda: None)
        self.history = ChatHistory(f"chat_history_{port}.json")

        # Peers: port -> PeerInfo (inclui desconectados conhecidos)
        self._peers: Dict[int, PeerInfo] = {}
        self._peers_lock = threading.Lock()

        self._seen_messages = set()
        self._seen_lock = threading.Lock()

        self._leader: Optional[int] = None
        self._leader_lock = threading.Lock()

        self.running = False
        self.server_socket: Optional[socket.socket] = None

        # Histórico de todos os nós vistos (para gossip mais rico)
        self._known_hosts: Dict[int, str] = {}  # port -> host
        self._known_names: Dict[int, str] = {}  # port -> name
        self._known_lock = threading.Lock()

        # Controla se já anunciamos JOIN nesta sessão (pertence ao Manager, não ao PeerInfo)
        self._join_announced = False
        self._join_lock = threading.Lock()

    # ─────────────────────────────── Lifecycle ──────────────────────────────

    def start(self):
        self.running = True

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(20)

        with self._known_lock:
            self._known_hosts[self.port] = self._get_local_ip()

        threading.Thread(target=self._accept_connections, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._failure_detector_loop, daemon=True).start()
        threading.Thread(target=self._udp_beacon_loop, daemon=True).start()
        threading.Thread(target=self._udp_listener_loop, daemon=True).start()
        threading.Thread(target=self._reconnect_loop, daemon=True).start()

        log.info(f"[{self.name}] Servidor TCP ativo na porta {self.port}")
        self._elect_leader()

    def stop(self):
        self.running = False
        leave_msg = Message(sender=self.name, content="", msg_type=MSG_LEAVE)
        self.broadcast(leave_msg)
        time.sleep(0.2)
        if self.server_socket:
            try: self.server_socket.close()
            except: pass
        with self._peers_lock:
            for info in self._peers.values():
                if info.sock:
                    try: info.sock.close()
                    except: pass
            self._peers.clear()

    # ─────────────────────────────── Conexão ────────────────────────────────

    def connect_to_peer(self, peer_host: str, peer_port: int,
                        announce_join: bool = False) -> bool:
        if peer_port == self.port:
            return False

        if self.port < peer_port:
            return False

        with self._peers_lock:
            if peer_port in self._peers and self._peers[peer_port].connected:
                return True

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((peer_host, peer_port))
            sock.settimeout(None)

            with self._known_lock:
                self._known_hosts[peer_port] = peer_host
                known_name = self._known_names.get(peer_port)

            info = PeerInfo(peer_host, peer_port, known_name)
            info.sock = sock
            info.connected = True
            info.last_seen = time.time()

            with self._peers_lock:
                self._peers[peer_port] = info

            # Handshake: manda nosso port + IP real
            hs_data = json.dumps({"port": self.port, "host": self._get_local_ip(), "name": self.name})
            hs_msg = Message(sender=str(self.port), content=hs_data, msg_type=MSG_HANDSHAKE)
            self._send_to_socket(sock, hs_msg)

            # Gossip: compartilha lista de todos nós conhecidos
            self._share_known_peers(sock)

            threading.Thread(target=self._listen_peer, args=(sock, peer_port), daemon=True).start()

            self._elect_leader()
            self.on_network_change_cb()
            log.info(f"[{self.name}] Conectado a peer_{peer_port} ({peer_host})")

            # Anuncia JOIN apenas uma vez por sessão (primeira conexão bem-sucedida)
            if announce_join:
                with self._join_lock:
                    if not self._join_announced:
                        self._join_announced = True
                        join_msg = Message(sender=self.name, content="", msg_type=MSG_JOIN)
                        self.broadcast(join_msg)
            return True
        except Exception as e:
            log.warning(f"[{self.name}] Falha ao conectar em {peer_host}:{peer_port} — {e}")
            return False

    def _share_known_peers(self, sock: socket.socket):
        """Envia lista completa de nós conhecidos (host:port:name) para fechamento de malha."""
        with self._known_lock:
            known_hosts = dict(self._known_hosts)
            known_names = dict(self._known_names)
        with self._peers_lock:
            peer_names = {p: info.name for p, info in self._peers.items()}

        known = {}
        for port, host in known_hosts.items():
            if port == self.port:
                continue
            name = peer_names.get(port) or known_names.get(port) or f"peer_{port}"
            known[str(port)] = {"host": host, "name": name}

        # Inclui a si mesmo para que o receptor conheça nosso nome
        known[str(self.port)] = {"host": self._get_local_ip(), "name": self.name}

        if not known:
            return
        msg = Message(sender=str(self.port), content=json.dumps(known), msg_type=MSG_PEER_EX)
        self._send_to_socket(sock, msg)

    # ─────────────────────────────── Broadcast ──────────────────────────────

    def broadcast(self, msg: Message):
        with self._seen_lock:
            self._seen_messages.add(msg.id)

        disconnected = []
        with self._peers_lock:
            peers_snapshot = {p: info for p, info in self._peers.items() if info.connected}

        for p_port, info in peers_snapshot.items():
            if not self._send_to_socket(info.sock, msg):
                disconnected.append(p_port)

        for p_port in disconnected:
            self._mark_disconnected(p_port)

    def _send_to_socket(self, sock: Optional[socket.socket], msg: Message) -> bool:
        if sock is None:
            return False
        try:
            payload = (msg.to_json() + "\n").encode("utf-8")
            sock.sendall(payload)
            return True
        except:
            return False

    # ─────────────────────────── Accept / Listen ────────────────────────────

    def _accept_connections(self):
        while self.running:
            try:
                self.server_socket.settimeout(1)
                sock, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_initial_handshake,
                                 args=(sock, addr[0]), daemon=True).start()
            except socket.timeout:
                continue
            except:
                break

    def _handle_initial_handshake(self, sock: socket.socket, remote_host: str):
        try:
            buffer = ""
            sock.settimeout(5)
            while self.running:
                data = sock.recv(4096).decode("utf-8")
                if not data:
                    return
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    msg = Message.from_json(line)
                    if msg.type == MSG_HANDSHAKE:
                        hs = json.loads(msg.content)
                        remote_port = int(hs["port"])
                        real_host   = hs.get("host", remote_host)
                        remote_name = hs.get("name", f"peer_{remote_port}")

                        info = PeerInfo(real_host, remote_port, remote_name)
                        info.sock = sock
                        info.connected = True
                        info.last_seen = time.time()

                        with self._peers_lock:
                            self._peers[remote_port] = info
                        with self._known_lock:
                            self._known_hosts[remote_port] = real_host
                            self._known_names[remote_port] = remote_name

                        sock.settimeout(None)
                        log.info(f"[{self.name}] Handshake aceito de {remote_name}")
                        threading.Thread(target=self._listen_peer,
                                         args=(sock, remote_port), daemon=True).start()

                        # Responde com nossos peers conhecidos
                        self._share_known_peers(sock)
                        self._elect_leader()
                        self.on_network_change_cb()

                        with self._join_lock:
                            if not self._join_announced:
                                self._join_announced = True
                                join_msg = Message(sender=self.name, content="", msg_type=MSG_JOIN)
                                threading.Thread(target=self.broadcast, args=(join_msg,), daemon=True).start()

                        return
        except Exception as e:
            log.debug(f"Handshake falhou: {e}")
            try: sock.close()
            except: pass

    def _listen_peer(self, sock: socket.socket, peer_port: int):
        buffer = ""
        while self.running:
            try:
                data = sock.recv(4096).decode("utf-8")
                if not data:
                    break
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        try:
                            msg = Message.from_json(line)
                            self._process_message(msg, sock, peer_port)
                        except Exception as e:
                            log.debug(f"Parse error: {e}")
            except:
                break
        self._mark_disconnected(peer_port)

    # ─────────────────────────── Process Messages ───────────────────────────

    def _process_message(self, msg: Message, sock: socket.socket, peer_port: int):
        # Atualiza last_seen
        with self._peers_lock:
            if peer_port in self._peers:
                self._peers[peer_port].last_seen = time.time()

        if msg.type == MSG_PING:
            pong = Message(sender=str(self.port), content="", msg_type=MSG_PONG)
            self._send_to_socket(sock, pong)
            return

        if msg.type == MSG_PONG:
            return  # last_seen já atualizado acima

        if msg.type == MSG_PEER_EX:
            try:
                known: dict = json.loads(msg.content)
                new_peers = []
                for port_str, value in known.items():
                    p = int(port_str)
                    if p == self.port:
                        continue
                    if isinstance(value, dict):
                        host = value["host"]
                        name = value.get("name", f"peer_{p}")
                    else:
                        host = value
                        name = f"peer_{p}"
                    with self._known_lock:
                        self._known_hosts[p] = host
                        self._known_names[p] = name
                    with self._peers_lock:
                        if p in self._peers:
                            self._peers[p].name = name
                        already = p in self._peers and self._peers[p].connected
                    if not already:
                        new_peers.append((host, p))

                # Conecta aos novos descobertos
                for host, p in new_peers:
                    threading.Thread(
                        target=self.connect_to_peer, args=(host, p, True), daemon=True
                    ).start()

                # Propaga o conhecimento atualizado para todos os outros conectados
                # (exceto quem nos enviou este PEER_EX)
                """
                if new_peers:
                    with self._peers_lock:
                        others = [info.sock for p, info in self._peers.items()
                                  if info.connected and p != peer_port]
                    for s in others:
                        self._share_known_peers(s)
                """

            except Exception as e:
                log.debug(f"PEER_EX parse error: {e}")
            return

        if msg.type == MSG_LEADER:
            try:
                announced = int(msg.content)
                with self._leader_lock:
                    if self._leader is None or announced >= self._leader:
                        self._leader = announced
                        log.info(f"[{self.name}] Líder atualizado via rede: peer_{announced}")
                self.on_network_change_cb()
            except:
                pass
            # Propaga o anúncio (flooding com dedup)
            with self._seen_lock:
                if msg.id in self._seen_messages:
                    return
                self._seen_messages.add(msg.id)
            if not msg.is_expired():
                self.broadcast(msg.increment_hops())
            return

        # Dedup para mensagens de chat/join/leave
        with self._seen_lock:
            if msg.id in self._seen_messages:
                return
            self._seen_messages.add(msg.id)

        if msg.type in (MSG_CHAT, MSG_JOIN, MSG_LEAVE):
            if msg.type == MSG_CHAT:
                self.history.save(msg)
            elif msg.type == MSG_LEAVE:
                # Identifica quem está saindo pelo nome e atualiza o estado
                with self._peers_lock:
                    ports_to_delete = []
                    for p, info in self._peers.items():
                        if info.name == msg.sender:
                            if info.connected:
                                # Se está conectado direto, avisa o detector de falhas que é uma saída limpa
                                info.clean_leave = True
                            else:
                                # Se já estava desconectado ou veio via gossip, remove para parar o reconnect_loop
                                ports_to_delete.append(p)
                    for p in ports_to_delete:
                        del self._peers[p]

            self.on_message_external_cb(msg)

        if not msg.is_expired():
            self.broadcast(msg.increment_hops())

    # ──────────────────────────── Eleição de Líder ──────────────────────────

    def _elect_leader(self):
        """
        Eleição simples: maior porta = líder.
        O novo líder é anunciado via flooding para toda a rede.
        """
        with self._peers_lock:
            connected_ports = [p for p, info in self._peers.items() if info.connected]
        all_nodes = connected_ports + [self.port]
        new_leader = max(all_nodes)

        changed = False
        with self._leader_lock:
            if self._leader != new_leader:
                self._leader = new_leader
                changed = True

        if changed:
            log.info(f"[{self.name}] Novo líder eleito: peer_{new_leader}")
            announce = Message(sender=str(self.port),
                               content=str(new_leader),
                               msg_type=MSG_LEADER)
            self.broadcast(announce)
            self.on_network_change_cb()

    # ─────────────────────────── Heartbeat / Detector ───────────────────────

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(3)
            ping = Message(sender=str(self.port), content="", msg_type=MSG_PING)
            with self._peers_lock:
                socks = [(p, info.sock) for p, info in self._peers.items() if info.connected]
            for p_port, sock in socks:
                self._send_to_socket(sock, ping)

    def _failure_detector_loop(self):
        while self.running:
            time.sleep(4)
            now = time.time()
            dead = []
            with self._peers_lock:
                for p, info in self._peers.items():
                    if info.connected and now - info.last_seen > 15:
                        dead.append(p)
            for p in dead:
                log.warning(f"[{self.name}] peer_{p} sem resposta — removendo")
                self._mark_disconnected(p)

    def _reconnect_loop(self):
        """Tenta reconectar periodicamente APENAS a nós que já estiveram conectados
        (existem em _peers), evitando spam de tentativas para nós UDP-discovered
        que nunca aceitaram conexão."""
        while self.running:
            time.sleep(8)
            with self._peers_lock:
                # Só reconecta a nós que já existem como PeerInfo (já houve handshake antes)
                to_reconnect = [
                    (info.host, port)
                    for port, info in self._peers.items()
                    if not info.connected
                ]

            for host, port in to_reconnect:
                threading.Thread(
                    target=self.connect_to_peer, args=(host, port), daemon=True
                ).start()

    def _mark_disconnected(self, peer_port: int):
        with self._peers_lock:
            if peer_port not in self._peers:
                return
            info = self._peers[peer_port]
            if not info.connected:
                return
            peer_name = info.name
            info.connected = False
            sock = info.sock
            info.sock = None
            
            # Verifica se foi uma saída intencional anunciada previamente
            was_clean = getattr(info, "clean_leave", False)
            if was_clean:
                # Remove do dicionário para que o _reconnect_loop nunca mais tente conectar
                del self._peers[peer_port]

        if sock:
            try: sock.close()
            except: pass

        log.info(f"[{self.name}] {peer_name} desconectado")

        # SÓ gera a notificação local se NÃO tiver sido uma saída limpa (anunciada por rede)
        if not was_clean:
            leave_msg = Message(sender=peer_name, content="", msg_type=MSG_LEAVE)
            with self._seen_lock:
                self._seen_messages.add(leave_msg.id)
            self.on_message_external_cb(leave_msg)

        self._elect_leader()
        self.on_network_change_cb()

    # ─────────────────────────── Descoberta UDP ─────────────────────────────

    def _get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"

    def _udp_beacon_loop(self):
        """Anuncia nossa presença via UDP broadcast a cada 5s."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            beacon = json.dumps({
                "type": "P2P_BEACON",
                "port": self.port,
                "name": self.name,
                "host": self._get_local_ip()
            }).encode()
            while self.running:
                try:
                    sock.sendto(beacon, ("<broadcast>", UDP_DISCOVERY_PORT))
                except:
                    pass
                time.sleep(5)
        except Exception as e:
            log.warning(f"UDP beacon error: {e}")

    def _udp_listener_loop(self):
        """Escuta beacons UDP de outros peers na rede local."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", UDP_DISCOVERY_PORT))
            sock.settimeout(1)
            while self.running:
                try:
                    data, addr = sock.recvfrom(1024)
                    beacon = json.loads(data.decode())
                    if beacon.get("type") != "P2P_BEACON":
                        continue
                    remote_port = int(beacon["port"])
                    remote_host = beacon.get("host", addr[0])
                    remote_name = beacon.get("name", f"peer_{remote_port}")

                    if remote_port == self.port:
                        continue

                    with self._known_lock:
                        self._known_hosts[remote_port] = remote_host
                        self._known_names[remote_port] = remote_name

                    # Tenta conectar automaticamente se ainda não conectado
                    with self._peers_lock:
                        already = remote_port in self._peers and self._peers[remote_port].connected
                    if not already:
                        threading.Thread(
                            target=self.connect_to_peer,
                            args=(remote_host, remote_port, True),
                            daemon=True
                        ).start()
                except socket.timeout:
                    continue
                except:
                    continue
        except Exception as e:
            log.warning(f"UDP listener error: {e}")

    # ─────────────────────────── API Pública ────────────────────────────────

    def list_connected_peers(self) -> List[dict]:
        with self._peers_lock:
            return [
                {"port": p, "host": info.host, "name": info.name, "connected": info.connected}
                for p, info in self._peers.items()
                if info.connected
            ]

    def list_discovered_peers(self) -> List[dict]:
        """Todos os nós vistos (descobertos via UDP ou gossip), conectados ou não."""
        with self._known_lock:
            known = dict(self._known_hosts)
            known_names = dict(self._known_names)
        with self._peers_lock:
            connected_ports = {p for p, info in self._peers.items() if info.connected}
            peer_names = {p: info.name for p, info in self._peers.items()}

        result = []
        for port, host in known.items():
            if port == self.port:
                continue
            name = peer_names.get(port) or known_names.get(port) or f"peer_{port}"
            result.append({
                "port": port,
                "host": host,
                "name": name,
                "connected": port in connected_ports
            })
        return result

    def get_leader(self) -> Optional[int]:
        with self._leader_lock:
            return self._leader

    def is_leader(self) -> bool:
        with self._leader_lock:
            return self._leader == self.port

    @property
    def peer_count(self) -> int:
        with self._peers_lock:
            return sum(1 for info in self._peers.values() if info.connected)

    def get_status(self) -> dict:
        leader = self.get_leader()
        if leader == self.port:
            leader_name = self.name
        else:
            with self._peers_lock:
                leader_name = self._peers[leader].name if leader and leader in self._peers else (f"peer_{leader}" if leader else "elegendo...")
        return {
            "name":       self.name,
            "port":       self.port,
            "is_leader":  self.is_leader(),
            "leader":     leader_name if leader else "elegendo...",
            "leader_port": leader,
            "peer_count": self.peer_count,
            "peers":      self.list_connected_peers(),
            "discovered": self.list_discovered_peers(),
        }