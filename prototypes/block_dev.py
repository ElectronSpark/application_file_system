from abc import ABC
from abc import abstractmethod


class BlockDevice(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def open(self):
        pass
    
    @abstractmethod
    def close(self):
        pass
    
    @abstractmethod
    def block_read(self, blk_id, blk_cnt):
        pass
    
    @abstractmethod
    def block_write(self, blk_id, blk_cnt):
        pass
    
    @abstractmethod
    def lock(self):
        pass
    
    @abstractmethod
    def unlock(self):
        pass
    