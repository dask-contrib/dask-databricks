import os
import uuid
from typing import Optional

from distributed.core import rpc
from distributed.deploy.cluster import Cluster
from tornado.ioloop import IOLoop

# Databricks Notebooks injects the `spark` session variable
if "spark" not in globals():
    spark = None


class DatabricksCluster(Cluster):
    """Connect to a Dask cluster deployed via databricks."""

    def __init__(
        self,
        loop: Optional[IOLoop] = None,
        asynchronous: bool = False,
    ):
        self.spark_local_ip = os.getenv("SPARK_LOCAL_IP")
        if self.spark_local_ip is None:
            raise KeyError(
                "Unable to find expected environment variable SPARK_LOCAL_IP. "
                "Are you running this on a Databricks driver node?"
            )
        try:
            name = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        except AttributeError:
            name = "unknown-databricks-" + uuid.uuid4().hex[:10]
        super().__init__(name=name, loop=loop, asynchronous=asynchronous)

        if not self.called_from_running_loop:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        self.scheduler_comm = rpc(f"{self.spark_local_ip}:8786")
        await super()._start()


def get_client():
    """Get a Dask client connected to a Databricks cluster."""
    return DatabricksCluster().get_client()
