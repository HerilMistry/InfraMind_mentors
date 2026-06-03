from shared.event_bus import Event

from recovery_agent.rollback import (
    RollbackManager
)


class RecoveryAgent:

    def __init__(
        self,
        context_store,
        event_bus
    ):

        self.context = context_store

        self.event_bus = event_bus

        self.rollback_manager = (
            RollbackManager()
        )

        self.event_bus.subscribe(
            "OPTIMIZATION_FAILED",
            self.recover
        )

    def recover(
        self,
        event
    ):

        print(
            "Recovery initiated..."
        )

        success = (
            self.rollback_manager.rollback(
                "inference-service"
            )
        )

        if success:

            self.context.set_last_action(
                "Rollback executed"
            )

            completed = Event(
                "ROLLBACK_COMPLETED"
            )

            self.context.add_event(
                completed
            )

            self.event_bus.publish(
                completed
            )