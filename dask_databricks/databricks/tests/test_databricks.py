import os

import pytest
from distributed.deploy import Cluster


from dask_databricks import databricks

@pytest.fixture
def remove_spark_local_ip():
    original_spark_local_ip = os.getenv("SPARK_LOCAL_IP")
    if original_spark_local_ip:
        del os.environ["SPARK_LOCAL_IP"]
    yield None
    if original_spark_local_ip:
        os.environ["SPARK_LOCAL_IP"] = original_spark_local_ip

@pytest.fixture
def set_spark_local_ip():
    original_spark_local_ip = os.getenv("SPARK_LOCAL_IP")
    os.environ["SPARK_LOCAL_IP"] = "1.1.1"
    yield None
    if original_spark_local_ip:
        os.environ["SPARK_LOCAL_IP"] = original_spark_local_ip
    else:
        del os.environ["SPARK_LOCAL_IP"]

def test_databricks_cluster_raises_key_error_when_initialised_outside_of_databricks(remove_spark_local_ip):
    with pytest.raises(KeyError):
        cluster = databricks.DatabricksCluster()

def test_databricks_cluster_creates_local_cluster_object(set_spark_local_ip):
    cluster = databricks.DatabricksCluster()
    assert isinstance(cluster, Cluster)
