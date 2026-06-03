from kubernetes import client, config


class LogParser:

    def __init__(self):

        try:
            config.load_kube_config()

        except Exception:
            config.load_incluster_config()

        self.v1 = client.CoreV1Api()

    def get_logs(
        self,
        namespace="default",
        pod_name=None,
        tail_lines=100
    ):

        if not pod_name:
            return ""

        try:

            return self.v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                tail_lines=tail_lines
            )

        except Exception as e:

            print(
                f"Failed to fetch logs: {e}"
            )

            return ""