from abc import ABC
from abc import abstractmethod


class SuperBlock(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def read(self):
        pass
    
    @abstractmethod
    def write(self):
        pass
    
    @abstractmethod
    def check(self):
        pass
