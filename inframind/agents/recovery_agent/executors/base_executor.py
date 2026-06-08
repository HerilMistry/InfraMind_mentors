"""Abstract executor for recovery actions.
All concrete executors must implement the `execute` method returning an
:class:`ExecutionReport`.
"""

from abc import ABC, abstractmethod
from typing import Any

from ..models import RecommendedAction, ExecutionReport, Recommendation


class ActionExecutor(ABC):
    """Base class for all executors.

    Sub‑classes decide how to perform the given ``action``. They receive the
    original :class:`Recommendation` (so they can inspect ``confidence`` and
    ``reasoning``) and must return an :class:`ExecutionReport`.
    """

    @abstractmethod
    def execute(self, recommendation: Recommendation) -> ExecutionReport:
        """Execute the recommended action.

        Parameters
        ----------
        recommendation:
            The full recommendation payload.
        """
        raise NotImplementedError
