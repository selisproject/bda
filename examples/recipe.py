from __future__ import print_function

import sys

from pyspark.sql import SparkSession

import random
import operator


###############################################################################
# Selis Recipe Library code.
###############################################################################

class SelisException(Exception):
    pass


class SelisDimensionTableService(object):
    '''A Selis Service that handles Dimension Tables.'''

    def __init__(self, session):
        self.session = session

        self.dimension_tables_jdbc_url = '{}{}'.format(
            self.session.dimension_tables_url, self.session.scn_database
        )

        self.properties = {
            'user': self.session.dimension_tables_user,
            'password': self.session.dimension_tables_pass,
        }

    def read(self, options):
        table = session.spark.read.jdbc(
            url=self.dimension_tables_jdbc_url,
            properties=self.properties,
            **options
        )

        return table


class SelisSession(object):
    '''A Selis Recipe Session.'''

    OPTIONS = {
        '--selis-scn-db': None,
        '--selis-dt-url': None,
        '--selis-dt-user': None,
        '--selis-dt-pass': None,
    }

    def __init__(self, args):
        for option in self.OPTIONS.iterkeys():
            try:
                option_index = args.index(option)
            except ValueError:
                raise SelisException('Uninitialized option: {}'.format(option))

            try:
                self.OPTIONS[option] = args[option_index + 1]
            except IndexError:
                raise SelisException('Uninitialized option: {}'.format(option))

        self.spark = SparkSession.builder.appName('TestRecipe').getOrCreate()

        self.dimension_tables = SelisDimensionTableService(self)

    @property
    def dimension_tables_url(self):
        return self.OPTIONS['--selis-dt-url']

    @property
    def dimension_tables_user(self):
        return self.OPTIONS['--selis-dt-user']

    @property
    def dimension_tables_pass(self):
        return self.OPTIONS['--selis-dt-pass']

    @property
    def scn_database(self):
        return self.OPTIONS['--selis-scn-db']

    def close(self):
        self.spark.stop()


###############################################################################
# Client Recipe code.
###############################################################################


def compute(spark, args):
    partitions = int(args[0]) if args else 2
    n = 100000 * partitions

    def f(_):
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(
        range(1, n + 1), partitions
    ).map(f).reduce(operator.add)

    return 4.0 * count / n


if __name__ == '__main__':
    session = SelisSession(sys.argv)

    print(80 * '-')
    print('(x) ----->{}'.format(sys.argv[1:]))
    print(80 * '-')

    rdd = session.dimension_tables.read({'table': 'warehouses'})

    print(80 * '-')
    print('(0) ----->{}'.format(rdd.first()))
    print(80 * '-')

    rdd = session.dimension_tables.read({
        'table': '(SELECT name FROM warehouses) q',
    })

    print(80 * '-')
    print('(1) ----->{}'.format(rdd.first()))
    print(80 * '-')

    result = compute(session.spark, sys.argv[1:])

    print(80 * '-')
    print('(2) ----->{}'.format(result))
    print(80 * '-')

    session.close()
