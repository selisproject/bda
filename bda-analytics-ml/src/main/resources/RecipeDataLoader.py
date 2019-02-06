import json
import psycopg2
from datetime import datetime

DIMENSION_TABLES_QUERY = '''\
    (SELECT * FROM {}) {}'''

KPI_DB_QUERY = '''\
    INSERT INTO {} (timestamp, result)\
    VALUES ('{}', '{}'::json)'''

def fetch_from_eventlog_one(spark, namespace, message_id, message_columns):
    '''Fetches messages from the EventLog.

    TODO: documentation.

    :param message_id:

    '''
    columns = message_columns.replace(" ", "").replace('[','').replace(']','').split(',')

    catalog = """
    {
        "table": {
            "namespace": \""""+namespace+"""\",
            "name": "Events"
        },
        "rowkey": "key",
        "columns": {
            "message_id":{"cf":"rowkey", "col":"key", "type":"string"},"""
    for column in columns:
        catalog+="""
            \""""+column+"""\":{"cf":"messages", "col":\""""+column+"""\", "type":"string"},"""
    catalog=catalog[0:-1]
    catalog+="""
        }
    }"""

    messages = spark.read.format("org.apache.spark.sql.execution.datasources.hbase").options(catalog=catalog).load()
    return messages.filter(messages["message_id"] == message_id).drop(messages["topic"]).drop(messages["message_id"])

def fetch_from_eventlog(spark, namespace, message_type, message_columns):
    '''Fetches messages from the EventLog.

    TODO: documentation.

    :param message_type:

    '''
    columns = message_columns.replace(" ", "").replace('[','').replace(']','').split(',')

    catalog = """
    {
        "table": {
            "namespace": \""""+namespace+"""\",
            "name": "Events"
        },
        "rowkey": "key",
        "columns": {
            "message_id":{"cf":"rowkey", "col":"key", "type":"string"},"""
    for column in columns:
        catalog+="""
            \""""+column+"""\":{"cf":"messages", "col":\""""+column+"""\", "type":"string"},"""
    catalog=catalog[0:-1]
    catalog+="""
        }
    }"""

    messages = spark.read.format("org.apache.spark.sql.execution.datasources.hbase").options(catalog=catalog).load()
    return messages.filter(messages["topic"] == message_type).drop(messages["topic"]).drop(messages["message_id"])

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

def save_results_to_kpi_database(kpi_db_url, username, password, kpi_table, message, result):
    '''Connects to KPI DB and stores the `results_list`.

    TODO: documentation.

    :param result:

    '''
    KPI_DB_SETTINGS = {
        'dbname': kpi_db_url,
        'host': 'selis-postgres',
        'port': '5432',
        'user': username,
        'password': password,
    }
    query = KPI_DB_QUERY.format(
        kpi_table,
        datetime.now(),
        json.dumps(result),
    )

    try:
        connection = psycopg2.connect(**KPI_DB_SETTINGS)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
    except Exception:
        print('Unable to connect to the database.')
        raise
    finally:
        cursor.close()
        connection.close()