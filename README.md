# dask-databricks

Cluster tools for running Dask on Databricks multi-node clusters.


## Quickstart

To launch a Dask cluster on Databricks you need to create an [init script](https://docs.databricks.com/en/init-scripts/index.html) with the following contents and configure your multi-node cluster to use it.

```bash
#!/bin/bash

# Install Dask + Dask Databricks
/databricks/python/bin/pip install --upgrade dask[complete] git+https://github.com/jacobtomlinson/dask-databricks.git@main

# Start Dask cluster components
dask databricks run
```

Then from your Databricks Notebook you can quickly connect a Dask `Client` to the scheduler running on the Spark Driver Node.

```python
import dask_databricks

client = dask_databricks.get_client()
```

Now you can submit work from your notebook to the multi-node Dask cluster.

```python
def inc(x):
    return x + 1

x = client.submit(inc, 10)
x.result()
```

### Dashboard

You can access the [Dask dashboard](https://docs.dask.org/en/latest/dashboard.html) via the Databricks driver-node proxy. The link can be found in `Client` or `DatabricksCluster` repr or via `client.dashboard_link`.

```python
>>> print(client.dashboard_link)
https://dbc-dp-xxxx.cloud.databricks.com/driver-proxy/o/xxxx/xx-xxx-xxxx/8087/status
```

![](https://user-images.githubusercontent.com/1610850/281442274-450d41c6-2eb6-42a1-8de6-c4a1a1b84193.png)

![](https://user-images.githubusercontent.com/1610850/281441285-9b84d5f1-d58a-45dc-9354-7385e1599d1f.png)

### Troubleshooting with cluster logs

If you're experiencing problems starting your Dask Databricks cluster then viewing logs for your init scripts can help narrow down the problem.

When you create your cluster we recommend that you [configure your logs](https://docs.databricks.com/en/clusters/configure.html#cluster-log-delivery) to write to somewhere like `dbfs:/cluster_init_logs`.

To make viewing these logs a little easier we've included a couple of CLI utilities in `dask-databricks` to help you navigate them.

#### Listing clusters

You can get a full list of available logs with the `dask databricks ls <path>` command where the path is the DBFS location you configured your logs to write to.

```console
$ dask databricks logs ls dbfs:/cluster_init_logs

  Cluster                Start time          Node Count   Node IPs
 ──────────────────────────────────────────────────────────────────────────────────────
  1234-987654-a1b2c3d4   Nov 16 2023 10:36   2            10.0.0.1, 10.0.0.2
```

#### Viewing logs

Once you have your cluster ID you can view the logs from the latest launch of that cluster with `dask databricks cat <path> <cluster>`.

```console
$ dask databricks logs cat dbfs:/cluster_init_logs 1234-987654-a1b2c3d4
Cluster: 1234-987654-a1b2c3d4
Start time: Nov 16 2023 10:36
10.0.0.1: Start Python bootstrap
10.0.0.1: PYSPARK_PYTHON is /databricks/python3/bin/python
...
```
