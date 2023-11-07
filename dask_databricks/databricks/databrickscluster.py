import os

from distributed.deploy.cluster import Cluster
from distributed.core import rpc
from typing import Optional
from tornado.ioloop import IOLoop

# Databricks Notebooks injects the `spark` session variable
if 'spark' not in globals():
    spark = None

class DatabricksCluster(Cluster):
    """Connect to a Dask cluster deployed via databricks."""

    def __init__(self,
        loop: Optional[IOLoop] = None,
        asynchronous: bool = False,):
        self.spark_local_ip =  os.getenv("SPARK_LOCAL_IP")
        if spark is None or self.spark_local_ip is None:
            raise KeyError("Unable to find expected environment variable SPARK_LOCAL_IP. "
                           "Are you running this on a Databricks driver node?")
        name = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        super().__init__(name=name, loop=loop, asynchronous=asynchronous)

        if not self.called_from_running_loop:
            self._loop_runner.start()
            self.sync(self._start)


    async def _start(self):
        self.scheduler_comm = rpc(f'{self.spark_local_ip}:8786')
        await super()._start()
