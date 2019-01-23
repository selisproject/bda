import json
from datetime import timedelta, datetime

from pyspark.sql import Row

DIMENSION_TABLES_QUERY = '''\
    (SELECT * FROM {}) {}'''

def fetch_from_eventlog_from_url(spark, dbname, message_id):
    '''Fetches messages from the EventLog.

    TODO: documentation.

    :param message_id:

    '''
    catalog = """
    {
        "table": {
            "namespace": \""""+dbname+"""\",
            "name": "Events"
        },
        "rowkey": "key",
        "columns": {
            "message_id":{"cf":"rowkey", "col":"key", "type":"string"},
            "message":{"cf":"messages", "col":"message", "type":"string"},
            "stock_levels_date":{"cf":"messages", "col":"stock_levels_date", "type":"string"},
            "supplier_id":{"cf":"messages", "col":"supplier_id", "type":"string"},
            "topic":{"cf":"messages", "col":"topic", "type":"string"},
            "warehouse_id":{"cf":"messages", "col":"warehouse_id", "type":"string"}
        }
    }"""

    messages = spark.read.format("org.apache.spark.sql.execution.datasources.hbase").options(catalog=catalog).load()
    return messages.filter(messages["message_id"] == message_id)

def fetch_from_master_data(spark, dimension_tables_url, username, password, table):
    '''Fetches master data from a Dimension table.

    TODO: documentation.

    :param message_id:

    '''
    query = DIMENSION_TABLES_QUERY.format(table,table)

    return spark.read.jdbc(
        url=dimension_tables_url,
        properties={'user':username,'password':password},
        table=query)

def save_results_to_kpi_database(spark, kpi_db_url, username, password, message, results_list, kpi_table):
    '''Connects to KPI DB and stores the `results_list`.

    TODO: documentation.

    :param results_list:

    '''
    result = spark.createDataFrame([(
        datetime.now(),
        message['supplier_id'],
        message['warehouse_id'],
        json.dumps(results_list))],
        ['timestamp', 'supplier_id', 'warehouse_id', 'result'])

    result.write.mode("append").jdbc(
        url=kpi_db_url,
        properties={'user':username,'password':password},
        table=kpi_table)