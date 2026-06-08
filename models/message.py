import uuid
import json
from datetime import datetime, timezone

MSG_CHAT     = "CHAT"
MSG_JOIN     = "JOIN"
MSG_LEAVE    = "LEAVE"
MSG_PING     = "PING"
MSG_PONG     = "PONG"
MSG_LEADER   = "LEADER_ANNOUNCE"
MSG_HANDSHAKE= "HANDSHAKE"
MSG_PEER_EX  = "PEER_EXCHANGE"

class Message:
    MAX_HOPS = 10

    def __init__(self, sender: str, content: str, msg_type: str = MSG_CHAT,
                 msg_id: str = None, timestamp: str = None, hops: int = 0):
        self.id        = msg_id or str(uuid.uuid4())
        self.type      = msg_type
        self.sender    = sender
        self.content   = content
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()
        self.hops      = hops

    def to_dict(self) -> dict:
        return {
            "id":        self.id,
            "type":      self.type,
            "sender":    self.sender,
            "message":   self.content,
            "timestamp": self.timestamp,
            "hops":      self.hops,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(raw: str) -> "Message":
        d = json.loads(raw)
        return Message(
            sender    = d["sender"],
            content   = d.get("message", ""),
            msg_type  = d.get("type", MSG_CHAT),
            msg_id    = d["id"],
            timestamp = d["timestamp"],
            hops      = d.get("hops", 0),
        )

    def increment_hops(self) -> "Message":
        return Message(self.sender, self.content, self.type,
                       self.id, self.timestamp, self.hops + 1)

    def is_expired(self) -> bool:
        return self.hops >= self.MAX_HOPS
