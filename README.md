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

Then from your Databricks Notebook you can use the `DatabricksCluster` class to quickly connect a Dask `Client` to the scheduler running on the Spark Driver Node.

```python
from dask.distributed import Client
from dask_databricks import DatabricksCluster

cluster = DatabricksCluster()
client = Client(cluster)
```

Now you can submit work from your notebook to the multi-node Dask cluster.

```python
def inc(x):
    return x + 1

x = client.submit(inc, 10)
x.result()
```
