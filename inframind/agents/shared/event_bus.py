from dataclasses import dataclass
from typing import Callable


@dataclass
class Event:
    event_type: str


class SimpleEventBus:

    def __init__(self):
        self._handlers = {}

    def subscribe(self,event_type: str,handler: Callable) -> None:

        if event_type not in self._handlers:
            self._handlers[event_type] = []

        self._handlers[event_type].append(handler)

    def publish(self,event: Event) -> None:

        for handler in self._handlers.get(
            event.event_type,
            []
        ):
            try:
                handler(event)

            except Exception as e:
                print(
                    f"Handler failed for "
                    f"{event.event_type}: {e}"
                )