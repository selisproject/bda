import sys
import json
from datetime import timedelta, datetime

import psycopg2
import requests

from pyspark.sql import Row


DATABASE_URLS = {
    'bda_server_url': None,
    'dimension_tables_url': None,
}

DIMENSION_TABLES_CREDENTIALS = {
    'user': None,
    'password': None,
}

KPI_DB_SETTINGS = {
    'dbname': None,
    'host': 'selis-postgres',
    'port': '5432',
    'user': None,
    'password': None,
}

OPTIONS = {
    '--selis-scn-name': None,
    '--selis-scn-slug': None,
    '--selis-scn-db': None,
    '--selis-kpi-db': None,
    '--selis-kpi-table': None,
    '--selis-dt-url': None,
    '--selis-dt-user': None,
    '--selis-dt-pass': None,
}

DIMENSION_TABLES_QUERY = '''\
    (SELECT * FROM {}) {}'''

KPI_DB_QUERY = '''\
    INSERT INTO {} (timestamp, supplier_id, warehouse_id, result)
    VALUES ('{}', {}, {}, '{}'::json)'''  # noqa


def fetch_from_eventlog_from_url(spark, message_id):
    '''Fetches messages from the EventLog (through the REST API).

    TODO: documentation.

    :param message_id:

    '''
    params = {
        'filters': 'key:{}'.format(message_id)
    }

    headers = {
        'Accept': 'application/json'
    }

    response = requests.get(
        DATABASE_URLS['bda_server_url'], params=params, headers=headers
    )

    # Convert json response to dataframe
    message_dataframe = spark.read.json(
        spark.sparkContext.parallelize(
            [json.dumps(response.json()[0]["tuple"])]
        )
    )

    # Get information from the message:
    #    (supplier, warehouse, date, stock values)
    message = {}

    message['supplier_id'] = message_dataframe.filter(
        message_dataframe.key.like('%supplier_id%')
    ).select("value").head()[0]

    message['warehouse_id'] = message_dataframe.filter(
        message_dataframe.key.like('%warehouse_id%')
    ).select("value").head()[0]

    message['stock_values_str'] = message_dataframe.filter(
        message_dataframe.key.like('%message%')
    ).select("value").head()[0]

    message['stock_levels_date'] = message_dataframe.filter(
        message_dataframe.key.like('%stock_levels_date%')
    ).select("value").head()[0]

    return message

def fetch_latest_forecast_from_eventlog_from_url(spark, message):
    '''Fetches the latest forecast from the EventLog (through the REST API).

    TODO: documentation.

    :param spark:

    '''
    filters = 'topic:SonaeSalesForecast;supplier_id:{};warehouse_id:{}'.format(
        message['supplier_id'], message['warehouse_id']
    )

    params = {
        'filters': filters
    }

    headers = {
        'Accept': 'application/json'
    }

    response = requests.get(
        DATABASE_URLS['bda_server_url'], params=params, headers=headers
    )

    # Convert json response to dataframe.
    forecast_dataframe = spark.read.json(
        spark.sparkContext.parallelize(
            [json.dumps(response.json()[0]["tuple"])]
        )
    )

    # Get actual sales forecast message.
    sales_forecast_str = forecast_dataframe.filter(
        forecast_dataframe.key.like('%message%')
    ).select("value").head()[0]

    return sales_forecast_str


def fetch_latest_forecast_from_eventlog_from_file(spark, filename):
    '''Fetches the latest forecast from the EventLog (from a local file).

    TODO: documentation.

    :param spark:
    :param filename:

    '''
    with open(filename) as f:
        message_file = f.readline()

    message_file = message_file.strip()

    # Convert file to dataframe
    fields = spark.sparkContext.parallelize(
        message_file[1:-1].split("],[")
    ).map(lambda l: l.split(","))

    forecast_dataframe = fields.map(
        lambda p: Row(name=p[0], value=','.join(p[1:]))
    ).toDF()

    # Get actual sales forecast message.
    sales_forecast_str = forecast_dataframe.filter(
        forecast_dataframe.name.like('%message%')
    ).select("value").head()[0]

    return sales_forecast_str

def save_results_to_kpi_database(message, results_list):
    '''Connects to KPI DB and stores the `results_list`.

    TODO: documentation.

    :param results_list:

    '''
    query = KPI_DB_QUERY.format(
        OPTIONS['--selis-kpi-table'],
        datetime.now(),
        message['supplier_id'],
        message['warehouse_id'],
        json.dumps(results_list),
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


def parse_arguments(args):
    '''Parses arguments. Returns the arguments to the actual recipe.

    :param args: The sys.argv arguments.
    :return: The recipe's actual arguments.

    '''
    for option in OPTIONS.keys():
        try:
            option_index = args.index(option)
        except ValueError:
            raise ValueError('Uninitialized option: {}'.format(option))

        try:
            OPTIONS[option] = args[option_index + 1]
        except IndexError:
            raise ValueError('Uninitialized option: {}'.format(option))

    recipe_arguments = []
    arguments_iterator = iter(args[1:])

    for argument in arguments_iterator:
        if argument not in OPTIONS:
            recipe_arguments.append(argument)
        else:
            next(arguments_iterator)

    return recipe_arguments


def initialize_recipe():
    '''Initializes global variables.

    It is based on OPTIONS, assumes it has been populated.

    :return: None

    '''
    DATABASE_URLS['bda_server_url'] = (
        'http://selis-controller:9999/api/datastore/{}/select'.format(
            OPTIONS['--selis-scn-slug']
        )
    )

    DATABASE_URLS['dimension_tables_url'] = '{}{}'.format(
        OPTIONS['--selis-dt-url'], OPTIONS['--selis-scn-db'],
    )

    DIMENSION_TABLES_CREDENTIALS['user'] = OPTIONS['--selis-dt-user']
    DIMENSION_TABLES_CREDENTIALS['password'] = OPTIONS['--selis-dt-pass']

    KPI_DB_SETTINGS['dbname'] = OPTIONS['--selis-kpi-db']
    KPI_DB_SETTINGS['user'] = OPTIONS['--selis-dt-user']
    KPI_DB_SETTINGS['password'] = OPTIONS['--selis-dt-pass']


if __name__ == '__main__':
    # Parse input arguments.
    recipe_arguments = parse_arguments(sys.argv)

    # Initialize parameters
    initialize_recipe()

    # Read stock levels message id as input.
    message_id = recipe_arguments[0]

    # Fetch message from the EventLog.
    message = fetch_from_eventlog_from_url(spark, message_id)

    # Convert stock values to a dataframe
    stock_values_dict = json.loads(message['stock_values_str'])

    stock_values_dataframe = spark.read.json(
        spark.sparkContext.parallelize(
            [json.dumps(stock_values_dict['stock_levels'])]
        )
    )

    sales_forecast_str = fetch_latest_forecast_from_eventlog_from_url(
        spark, message
    )

    # Convert sales forecast message from string to dataframe.
    sales_forecast_dict = json.loads(sales_forecast_str)
    forecast_dataframe = spark.read.json(
        spark.sparkContext.parallelize(
            [json.dumps(sales_forecast_dict["sales_forecast"])]
        )
    )


    # Get required sku info from dimension tables.
    query = DIMENSION_TABLES_QUERY.format("")

    skus_dataframe = spark.read.jdbc(
        url=DATABASE_URLS['dimension_tables_url'],
        properties=DIMENSION_TABLES_CREDENTIALS,
        table=query,)


    save_results_to_kpi_database(message, results_final_list)