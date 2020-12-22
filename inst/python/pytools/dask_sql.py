import dask
import distributed
import pandas as pd
from dask.datasets import timeseries
import dask.dataframe as dd

# Create a context to hold the registered tables
def create_context():
    from dask_sql import Context
    ctx = Context()
    ctx.dors_client = False
    return ctx

def create_context_distributed():
    import sys
    print(sys.path)
    from dask_sql import Context
    from dask.distributed import Client, LocalCluster

    # Need dev version 1.18+ of reticulate
    # Error: C stack usage   is too close to the limit
    # devtools::install_github('rstudio/reticulate')
    client = Client()
    ctx = Context()

    # FIX: develop better client handling on the R side
    ctx.dors_client = client
    return ctx

def create_blazing_context():
    from blazingsql import BlazingContext
    from dask.distributed import Client
    from dask_cuda import LocalCUDACluster

    cluster = LocalCUDACluster()
    # Need dev version 1.18+ of reticulate
    # Error: C stack usage  193896484868 is too close to the limit
    client = Client(cluster)
    ctx = BlazingContext(dask_client = client, network_interface = 'lo')
    
    # FIX: develop better client handling on the R side
    ctx.dors_client = client
    return ctx

# def create_sched():
#     from dask.distributed import Scheduler
    

def create_data():
    df = timeseries()
    return df

def create_table(c, name, df, npartitions=1):
    if 'blazing' in repr(c).lower():
        c.create_table(name, df)
        print(c.tables)
    if isinstance(df, pd.DataFrame):
        df = dd.from_pandas(df, npartitions=npartitions)
        c.create_table(name, df)
    else:
        c.create_table(name, df)

def run_sql(c, sql, compute=True):
    res = c.sql(sql)
    if isinstance(res, dd.DataFrame) and compute:
        res = res.compute()
    return res

