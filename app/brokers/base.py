from abc import ABC, abstractmethod

class BrokerBase(ABC):
    @abstractmethod
    def place_order(self, order): ...
