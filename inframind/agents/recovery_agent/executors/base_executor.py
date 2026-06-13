

from abc import ABC, abstractmethod
from typing import Any

from ..models import RecommendedAction, ExecutionReport, Recommendation


class ActionExecutor(ABC):
    

    @abstractmethod
    def execute(self, recommendation: Recommendation) -> ExecutionReport:
        
        raise NotImplementedError
