import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from shared.context_store import ContextStore
from shared.event_bus import SimpleEventBus

from monitoring_agent.monitoring_agent import (
    MonitoringAgent
)

from root_cause_agent.analyzer import (
    RootCauseAgent
)

from optimization_agent.optimizer import (
    OptimizationAgent
)

from recovery_agent.recovery_manager import (
    RecoveryAgent
)


def main():

    context = ContextStore()

    event_bus = SimpleEventBus()


    root_cause_agent = RootCauseAgent(
        context,
        event_bus
    )

    optimization_agent = OptimizationAgent(
        context,
        event_bus
    )

    recovery_agent = RecoveryAgent(
        context,
        event_bus
    )

    monitoring_agent = MonitoringAgent(
        context,
        event_bus
    )

    print(
        "\n=== Starting InfraMind ===\n"
    )

    # Start the chain
    monitoring_agent.run()

    print(
        "\n=== Context Store ==="
    )

    print(
        f"\nMetrics:\n{context.metrics}"
    )

    print(
        f"\nEvents:\n{context.events}"
    )

    print(
        f"\nRoot Cause:\n{context.root_cause}"
    )

    print(
        f"\nLast Action:\n{context.last_action}"
    )


if __name__ == "__main__":
    main()