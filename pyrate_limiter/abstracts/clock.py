from abc import ABC, abstractmethod
from typing import Awaitable


class AbstractClock(ABC):
    """Clock that return timestamp for `now`"""

    @abstractmethod
    def now(self) -> int | Awaitable[int]:
        """Get time as of now, in miliseconds"""


class AsyncAbstractClock(AbstractClock):
    @abstractmethod
    async def now(self) -> int:
        """Get time as of now, in miliseconds"""
