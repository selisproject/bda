import json
import psycopg2
import ssl
import urllib.request

from datetime import datetime

DIMENSION_TABLES_QUERY = '''\
    (SELECT * FROM {}) {}'''

KPI_DB_QUERY = '''\
    INSERT INTO {} (timestamp, result{})\
    VALUES ('{}', '{}'::json{})'''

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
    return messages.filter(messages["message_id"] == message_id).drop(messages["message_type"]).drop(messages["message_id"]).drop(messages["scn_slug"])

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
    return messages.filter(messages["message_type"] == message_type).drop(messages["message_type"]).drop(messages["message_id"]).drop(messages["scn_slug"])

def fetch_from_master_data(spark, dimension_tables_url, username, password, table):
    '''Fetches master data from a Dimension table.

    TODO: documentation.

    :param message_id:

    '''
    query = DIMENSION_TABLES_QUERY.format(table,table)

    return spark.read.jdbc(
        url=dimension_tables_url,
        properties={'user':username,'password':password,'driver': 'org.postgresql.Driver'},
        table=query)

def save_result_to_kpidb(kpidb_host, kpidb_port, kpidb_name, username, password, kpi_table, message, message_columns, result):
    '''Connects to KPI DB and stores the `results_list`.

    TODO: documentation.

    :param result:

    '''

    KPI_DB_SETTINGS = {
        'dbname': kpidb_name,
        'host': kpidb_host,
        'port': kpidb_port,
        'user': username,
        'password': password,
    }

    columns_str = ""
    if message_columns != "":
        columns = message_columns.replace(" ", "").replace('[','').replace(']','').split(',')
        columns.remove("payload")
        columns.remove("message_type")
        columns.remove("scn_slug")
        columns_str = ','+','.join(columns)


    fields = []
    fields_str = ""
    if message != '':
        message_data = message.collect()[0]
        for column in columns:
            fields.append(message_data[column])
        fields_str = ",'"+"','".join(fields)+"'"

    query = KPI_DB_QUERY.format(
        kpi_table,
        columns_str,
        datetime.now(),
        json.dumps(result),
        fields_str
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
    print("Result saved in KPI db")

def publish_result(pubsub_address, pubsub_port, pubsub_cert, scn_slug, message_type, result):
    PUBSUB_SERVICE_URL = 'https://'+pubsub_address+':'+pubsub_port+'/publish'
    ctx = ssl.create_default_context(cafile=pubsub_cert)

    headers = {
        'Content-Type': 'application/json'
    }
    message = {
        'message_type': message_type,
        'scn_slug': scn_slug,
        'payload': result
    }

    request = urllib.request.Request(PUBSUB_SERVICE_URL, json.dumps(message).encode('utf-8'), headers)
    response = urllib.request.urlopen(request, context=ctx)

    print(response.code)
    print(response.read().decode("utf8"))
    response.close()
