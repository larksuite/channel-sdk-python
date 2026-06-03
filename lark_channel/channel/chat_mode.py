import time
from collections import OrderedDict
from typing import Optional

from .config import ChatModeCacheConfig


class ChatModeCache:
    def __init__(self, config: ChatModeCacheConfig) -> None:
        self._config = config
        self._values = OrderedDict()

    def get(self, chat_id: str) -> Optional[str]:
        if not self._config.enabled:
            return None
        item = self._values.get(chat_id)
        if item is None:
            return None
        value, expires_at = item
        if expires_at < time.time():
            self._values.pop(chat_id, None)
            return None
        self._values.move_to_end(chat_id)
        return value

    def set(self, chat_id: str, chat_mode: str) -> None:
        if not self._config.enabled or self._config.max_size <= 0:
            return
        self._values[chat_id] = (chat_mode, time.time() + self._config.ttl_seconds)
        self._values.move_to_end(chat_id)
        while len(self._values) > self._config.max_size:
            self._values.popitem(last=False)
