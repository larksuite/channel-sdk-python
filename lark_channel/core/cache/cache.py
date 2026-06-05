from abc import ABC, abstractmethod


class ICache(ABC):
    @abstractmethod
    def get(self, key: str) -> str:
        pass

    # expire: expiration time, Unix timestamp (seconds)
    @abstractmethod
    def set(self, key: str, value: str, expire: int):
        pass
