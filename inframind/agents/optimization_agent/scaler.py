from kubernetes import client, config


class K8sOptimizer:

    def __init__(self):

        try:
            config.load_kube_config()

        except Exception:
            config.load_incluster_config()

        self.apps = client.AppsV1Api()

    def restart_deployment(
        self,
        deployment_name,
        namespace="default"
    ):

        try:

            deployment = (
                self.apps.read_namespaced_deployment(
                    deployment_name,
                    namespace
                )
            )

            deployment.spec.template.metadata.annotations = {
                "kubectl.kubernetes.io/restartedAt":
                "now"
            }

            self.apps.patch_namespaced_deployment(
                deployment_name,
                namespace,
                deployment
            )

            return True

        except Exception as e:

            print(
                f"Restart failed: {e}"
            )

            return False