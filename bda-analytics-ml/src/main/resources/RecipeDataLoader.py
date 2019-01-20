import sys
import json
from datetime import timedelta, datetime

import psycopg2

from pyspark.sql import Row

DIMENSION_TABLES_QUERY = '''\
    (SELECT * FROM {}) {}'''

KPI_DB_QUERY = '''\
    INSERT INTO {} (timestamp, supplier_id, warehouse_id, result)
    VALUES ('{}', {}, {}, '{}'::json)'''  # noqa


def fetch_from_eventlog_from_url(spark, message_id, eventlog_url, scn_slug):
    '''Fetches messages from the EventLog (through the REST API).

    TODO: documentation.

    :param message_id:

    '''


    return message

def fetch_from_master_data(spark, dimension_tables_url, username, password, table):
    # Get required sku info from dimension tables.
    query = DIMENSION_TABLES_QUERY.format(table,table)

    return spark.read.jdbc(
        url=dimension_tables_url,
        properties={'user':username,'password':password},
        table=query)

def save_results_to_kpi_database(message, results_list, kpi_table):
    '''Connects to KPI DB and stores the `results_list`.

    TODO: documentation.

    :param results_list:

    '''
    query = KPI_DB_QUERY.format(
        kpi_table,
        datetime.now(),
        message['supplier_id'],
        message['warehouse_id'],
        json.dumps(results_list),
    )

    try:
        connection = psycopg2.connect() # TODO: FILL db info

        cursor = connection.cursor()

        cursor.execute(query)

        connection.commit()
    except Exception:
        print('Unable to connect to the database.')
        raise
    finally:
        cursor.close()
        connection.close()