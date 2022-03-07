from pyflink.table import TableEnvironment, EnvironmentSettings, SqlDialect
from pyflink.table.catalog import HiveCatalog


def log_processing():
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)
    # t_env.get_config().set_sql_dialect(SqlDialect.HIVE)

    catalog_name = "myhive"
    default_database = "cpu"
    hive_conf_dir = "/etc/hive/conf.cloudera.hive"

    hive_catalog = HiveCatalog(catalog_name, default_database, hive_conf_dir)
    t_env.register_catalog("myhive", hive_catalog)

    # set the HiveCatalog as the current catalog of the session
    t_env.use_catalog("myhive")

    # print(t_env.list_catalogs())
    #

    t_env.execute_sql('SHOW CURRENT CATALOG').print()

    # t_env.get_config().set_sql_dialect(SqlDialect.HIVE)

    # t_env.execute_sql("""
    #     CREATE TABLE IF NOT EXISTS sink_table (name STRING, age INT) WITH (
    #       'connector.type' = 'kafka',
    #       'connector.version' = 'universal',
    #       'connector.topic' = 'test',
    #       'connector.properties.bootstrap.servers' = 'm.cdh:9092,n1.cdh:9092,n2.cdh:9092',
    #       'format.type' = 'csv',
    #       'update-mode' = 'append'
    #     )
    #     """).print()

    # t_env.execute_sql("""
    #     CREATE TABLE IF NOT EXISTS sink_table(
    #         `a` FLOAT
    #     ) WITH (
    #       'connector' = 'kafka',
    #       'topic' = 'sink_topic',
    #       'properties.bootstrap.servers' = 'm.cdh:9092,n1.cdh:9092,n2.cdh:9092',
    #       'scan.startup.mode' = 'earliest-offset',
    #       'format' = 'json'
    #     )
    #     """).print()

    t_env.sql_query("SELECT `value` AS `a` FROM cpu_idle LIMIT 10") \
        .execute_insert("sink_table").wait()

    t_env.execute_sql("SELECT * FROM cpu_idle LIMIT 5").print()

    df = t_env.sql_query("SELECT * FROM cpu_idle LIMIT 5").to_pandas()

    import sys
    print(sys.getsizeof(df.time_stamp[0]))

    # t_env.execute_sql("SELECT * FROM sink_table")


if __name__ == '__main__':
    log_processing()
