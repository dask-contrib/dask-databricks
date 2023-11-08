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
