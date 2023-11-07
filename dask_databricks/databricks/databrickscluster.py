import os

from distributed.deploy import LocalCluster
from distributed.core import rpc

class DatabricksCluster(LocalCluster):
    """Connect to a Dask cluster deployed via databricks."""

    def __init__(self):
        spark_local_ip = os.getenv("SPARK_LOCAL_IP")
        if spark_local_ip is None:
            raise KeyError("Unable to find expected environment variable SPARK_LOCAL_IP. "
                           "Are you running this on a Databricks driver node?")
        super().__init__(f'{spark_local_ip}:8786')
