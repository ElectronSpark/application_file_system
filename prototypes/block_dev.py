from abc import ABC
from abc import abstractmethod
from typing import Optional


class BlockDevice(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def open(self) -> bool:
        pass
    
    @abstractmethod
    def close(self) -> bool:
        pass
    
    @abstractmethod
    def block_read(self, blk_id: int, blk_cnt: int) -> Optional['bytearray']:
        pass
    
    @abstractmethod
    def block_write(self, blk_id: int, blk_cnt: int, buf: bytes) -> int:
        pass
    
    @abstractmethod
    def flush(self) -> bool:
        pass
    
    @abstractmethod
    def lock(self) -> bool:
        pass
    
    @abstractmethod
    def unlock(self) -> None:
        pass
    