from pyflink.table import *
from pyflink.table.catalog import HiveCatalog

from src.dimensional.dimen_resize import ReSize
from src.mafs.models.Prophet import MAFSProphetModel
import pandas as pd

settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

catalog_name = "myhive"
default_database = "cpu"
hive_conf_dir = "/etc/hive/conf.cloudera.hive"

hive_catalog = HiveCatalog(catalog_name, default_database, hive_conf_dir)
t_env.register_catalog("myhive", hive_catalog)

# set the HiveCatalog as the current catalog of the session
t_env.use_catalog("myhive")

# t_env.execute_sql('SELECT * FROM cpu.cpu_idle')
df = t_env.from_path('cpu.cpu_idle').to_pandas()

df = df[['endpoint', 'time_stamp', 'value']]

df = df.groupby(['endpoint'])

for x in df:
    import time

    start_time = time.time()

    a = x[1][['time_stamp', 'value']]
    # a['time_stamp'] = a['time'].apply(lambda y: pd.Timestamp(y))
    # a.drop('time', axis=1, inplace=True)
    MAFSProphetModel(ReSize(a, step=300, method='PAA').get_data()).predict(step=300)

    print('消耗时间:', time.time() - start_time, '秒')

