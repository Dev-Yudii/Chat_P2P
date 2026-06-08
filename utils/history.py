import json
import threading
from pathlib import Path
from typing import List
from models.message import Message

class ChatHistory:
    MAX_MESSAGES = 500

    def __init__(self, filepath: str):
        self._path = Path(filepath)
        self._lock = threading.Lock()
        self._messages: List[dict] = self._load()

    def _load(self) -> List[dict]:
        if not self._path.exists():
            return []
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except:
            return []

    def _persist(self):
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._messages, f, ensure_ascii=False, indent=2)
        except:
            pass

    def save(self, msg: Message):
        with self._lock:
            self._messages.append(msg.to_dict())
            if len(self._messages) > self.MAX_MESSAGES:
                self._messages = self._messages[-self.MAX_MESSAGES:]
            self._persist()

    def get_last(self, n: int = 100) -> List[dict]:
        with self._lock:
            return list(self._messages[-n:])

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._messages)
