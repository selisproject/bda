import pandas as pd

import pyspark
import datetime, time
from pyspark.sql.functions import to_timestamp, udf, col, unix_timestamp, from_unixtime
from pyspark.sql.types import LongType
from datetime import datetime, timedelta


def _timediff_x(dates):
    date11 = datetime.strptime(dates[0], "%Y-%m-%d %H:%M:%S")
    date22 = datetime.strptime(dates[1], "%Y-%m-%d %H:%M:%S")
    return ( date22- date11).total_seconds() /timedelta(hours=1).total_seconds()

def run(spark, data, barges, date_from,  date_to, mmsi):
#    print(date_from)
#    print(date_to)
#    barges.show()
#    print(barges.head(1))
    input_data = barges.select("payload")
    df = spark.read.json(input_data.rdd.map(lambda r: r.payload))
    df = df.filter(col('mmsi')==int(mmsi))
#    dataframe.show()
# create filter
    dates = (date_from,date_to)
    timestamps = (time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple()) for s in dates)


# read 'in' and 'out' as timestamps
    df = df.select(to_timestamp('in', 'yyyy-MM-dd HH:mm:ss').alias('in'), to_timestamp('out', 'yyyy-MM-dd HH:mm:ss').alias('out'), 'terminals', 'type', 'dif')
    q2 = """CAST(in AS INT)
            BETWEEN unix_timestamp('{0}', 'yyyy-MM-dd HH:mm:ss')
            AND unix_timestamp('{1}', 'yyyy-MM-dd HH:mm:ss')""".format(*dates)
    filtered=df.where(q2)
    aggregate=filtered.groupBy("type").agg({'dif':'sum'})
    hsum = aggregate.agg({'sum(dif)':'sum'}).collect()[0][0]
    newRow = spark.createDataFrame([('Sailing', _timediff_x(dates) - hsum)], aggregate.columns)
    appended = aggregate.union  (newRow)
#    appended.show()
    app = appended.toPandas().to_json(orient='records')
    print(app)
    return(app)

