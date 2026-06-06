from shared.event_bus import Event

from optimization_agent.scaler import (
    K8sOptimizer
)


class OptimizationAgent:

    def __init__(
        self,
        context_store,
        event_bus
    ):

        self.context = context_store

        self.event_bus = event_bus

        self.optimizer = (
            K8sOptimizer()
        )

        self.event_bus.subscribe(
            "ROOT_CAUSE_FOUND",
            self.optimize
        )

    def optimize(
        self,
        event
    ):

        print(
            "Optimization started..."
        )

        success = (
            self.optimizer.restart_deployment(
                deployment_name=
                "inference-deployment"
            )
        )

        if success:

            action = (
                "Restarted deployment"
            )

        else:

            action = (
                "Restart failed"
            )

        self.context.set_last_action(
            action
        )
        if success:

            completed = Event(
        "OPTIMIZATION_COMPLETED"
            )

        else:

            completed = Event(
        "OPTIMIZATION_FAILED"
            )

        self.context.add_event(
            completed
        )

        self.event_bus.publish(
            completed
        )

        print(action)