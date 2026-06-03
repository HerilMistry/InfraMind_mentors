class ContextStore:

    def __init__(self):

        self.metrics = {}

        self.logs = {}

        self.events = []

        self.root_cause = None

        self.last_action = None

    def update_metrics(
        self,
        data
    ):
        self.metrics = data

    def add_event(
        self,
        event
    ):
        self.events.append(event)

    def set_root_cause(
        self,
        cause
    ):
        self.root_cause = cause

    def set_last_action(
        self,
        action
    ):
        self.last_action = action