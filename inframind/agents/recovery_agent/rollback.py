class RollbackManager:

    def rollback(
        self,
        deployment_name
    ):

        print(
            f"Rolling back "
            f"{deployment_name}"
        )

        return True