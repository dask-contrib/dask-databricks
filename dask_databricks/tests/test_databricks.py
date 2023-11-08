import os

import pytest
from dask.distributed import Client
from distributed.deploy import Cluster, LocalCluster

from dask_databricks import DatabricksCluster, get_client


@pytest.fixture(scope="session")
def dask_cluster():
    """Start a LocalCluster to simulate the cluster that would be started on Databricks."""
    return LocalCluster(scheduler_port=8786)


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
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    yield None
    if original_spark_local_ip:
        os.environ["SPARK_LOCAL_IP"] = original_spark_local_ip
    else:
        del os.environ["SPARK_LOCAL_IP"]


def test_databricks_cluster_raises_key_error_when_initialised_outside_of_databricks(remove_spark_local_ip):
    with pytest.raises(KeyError):
        DatabricksCluster()


def test_databricks_cluster_create(set_spark_local_ip, dask_cluster):
    cluster = DatabricksCluster()
    assert isinstance(cluster, Cluster)


def test_databricks_cluster_create_client(set_spark_local_ip, dask_cluster):
    cluster = DatabricksCluster()
    client = Client(cluster)
    assert isinstance(client, Client)
    assert client.submit(sum, (10, 1)).result() == 11


def test_get_client(set_spark_local_ip, dask_cluster):
    client = get_client()
    assert isinstance(client, Client)
    assert isinstance(client.cluster, DatabricksCluster)
    assert client.submit(sum, (10, 1)).result() == 11
