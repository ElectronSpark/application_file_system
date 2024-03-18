from abc import ABC


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
