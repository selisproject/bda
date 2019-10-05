import numpy as np
import datetime as dt
from datetime import datetime

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GBTRegressor




def extract(row, colname):
		return tuple(map(lambda x: row[x], row.__fields__)) + tuple(row[colname + "_ohe"].toArray().tolist())

def run(spark, message_dataframe, timetable, stations, ris, train_dict):

	jsonSchema = spark.read.json(message_dataframe.rdd.map(lambda r: r.payload)).schema
	trainEvent = message_dataframe.withColumn('json', from_json(col('payload'), jsonSchema)).select("station_id", "ris_status_id", "timetable_id", "json")

	trainEvent = trainEvent.select(trainEvent.station_id, 
			trainEvent.ris_status_id, 
			trainEvent.timetable_id, 
			trainEvent.json.info.event_datetime.alias("datetime"), 			
			trainEvent.json.info.train_id.alias("train_id"))

	ris_status_id = trainEvent.collect()[0]["ris_status_id"]
	station_id = trainEvent.collect()[0]["station_id"]
	timetable_id = trainEvent.collect()[0]["timetable_id"]
	datetime_str =  trainEvent.collect()[0]["datetime"]

	ris_status_name = spark.sql("select name from ris where id == " + str(ris_status_id)).collect()[0]["name"]

	trainEvent.registerTempTable("trainEvent")
	trainEvent = spark.sql("select station_id, hour(datetime) as hour, dayofmonth(datetime) AS day, month(datetime) as month from trainEvent")

	feature_columns = ["station_id", "hour", "day", "month"]
	keep_columns = [c for c in train_dict["keepStationColumns"] if c != "dur_stop"]

	medians = dict(list(map(lambda x: (float(x.station_id), x.median), train_dict["medianTimePerStation"].collect())))
	stops_time = 0.0
	if ris_status_name == "Arrival":
		transTrainEvent = train_dict["pipelineIndexerModel"].transform(trainEvent)
		encodedTrainEvent = train_dict["oheStationModel"].transform(transTrainEvent)
		for colname in feature_columns:
			colIdx =  train_dict["stationNameDict"][colname]
			newCols = list(map(lambda x: str(x[1]).split(".")[0], colIdx))
			actualCol = encodedTrainEvent.columns
			allColNames = actualCol + newCols
			encodedTrainEvent = encodedTrainEvent.rdd.map(lambda x : extract(x, colname)).toDF(allColNames)
		encodedTrainEvent = encodedTrainEvent[keep_columns]
		encodedTrainEvent = train_dict["stationTrainAssembler"].transform(encodedTrainEvent)
		encodedTrainEvent = encodedTrainEvent.select("features")
		prediction = train_dict["stationTrainModel"].transform(encodedTrainEvent)
		stops_time += prediction.collect()[0].prediction


	station_rows = spark.sql("select station_id, station_order from timetable where timetable_id == " + timetable_id + " and station_order > (select station_order from timetable where station_id == '" + str(station_id) + "') and station_order < (select max(station_order) from timetable where timetable_id == " + timetable_id + ") order by station_order").collect()
	station_rows = list(map(lambda x : x.station_id, station_rows))
	
	for station in station_rows:
		if station in medians.keys():
			stops_time += medians[station]
		else:
			stops_time += spark.sql("select departure_second_of_the_day - arrival_second_of_the_day as stop from timetable where timetable_id == " + str(timetable_id) + " and station_id ==  " + str(int(station))).collect()[0]["stop"]

	next_route = spark.sql("select start_lon, start_lat, end_lon, end_lat, hour, day, month from (select longitude as start_lon, latitude as start_lat, hour(e.datetime) as hour, dayofmonth(e.datetime) AS day, month(e.datetime) as month, t.station_order as ord, e.timetable_id as tid from trainEvent as e, stations as s, timetable as t where e.station_id == s.id and t.timetable_id == e.timetable_id and t.station_id == e.station_id) as s, (select longitude as end_lon, latitude as end_lat, station_order as ord, timetable_id as tid from timetable as t, stations as s where t.station_id == s.id) as e where s.tid == e.tid and s.ord + 1 == e.ord")

	feature_columns = ["hour", "day", "month"]
	keep_columns = [c for c in train_dict["keepRouteColumns"] if c != "dur_route"]

	routes_time = 0.0
	next_route = train_dict["pipelineIndexerModel"].transform(next_route)
	encoded_train_route_data = train_dict["oheRouteModel"].transform(next_route)
	for colname in feature_columns:
        	colIdx = train_dict["routeNameDict"][colname]
	        newCols = list(map(lambda x: str(x[1]).split(".")[0], colIdx))
	        actualCol = encoded_train_route_data.columns
	        allColNames = actualCol + newCols
	        encoded_train_route_data = encoded_train_route_data.rdd.map(lambda x : extract(x, colname)).toDF(allColNames)

	encoded_train_route_data = encoded_train_route_data[keep_columns]
	encoded_train_route_data = train_dict["routeTrainAssembler"].transform(encoded_train_route_data)
	encoded_train_route_data = encoded_train_route_data.select("features")
	prediction = train_dict["routeTrainModel"].transform(encoded_train_route_data)
	routes_time += prediction.collect()[0].prediction

	rest_routes = spark.sql("select start_lon, start_lat, end_lon, end_lat from (select s.longitude as start_lon, s.latitude as start_lat, t.station_order as ord from timetable as t, stations as s, trainEvent as e where e.timetable_id == t.timetable_id and t.station_id == s.id and t.station_order > (select station_order from trainEvent as e, timetable as t where e.station_id == t.station_id and e.timetable_id == t.timetable_id)) as s, (select s.longitude as end_lon, s.latitude as end_lat, t.station_order as ord from timetable as t, stations as s, trainEvent as e where e.timetable_id == t.timetable_id and t.station_id == s.id and t.station_order > (select station_order from trainEvent as e, timetable as t where e.station_id == t.station_id and e.timetable_id == t.timetable_id)) as e where s.ord + 1 = e.ord")

	rest_routes = train_dict["routePartialTrainAssembler"].transform(rest_routes)
	rest_routes = rest_routes.select('features')
	predictions = train_dict["routePartialTrainModel"].transform(rest_routes)
	routes_time += predictions.groupBy().sum().collect()[0][0]

	total_time = stops_time + routes_time
	eta = str((datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S') + dt.timedelta(0, seconds = total_time)))
	print(eta)
	return(eta)


