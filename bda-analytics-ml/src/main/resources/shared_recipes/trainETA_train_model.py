import numpy as np
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer
from pyspark.sql.functions import udf
from pyspark.sql.functions import array_contains, col, explode
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GBTRegressor

def extract(row, colname):
		return tuple(map(lambda x: row[x], row.__fields__)) + tuple(row[colname + "_ohe"].toArray().tolist())

def run(spark, timetable, stations, ris, trainEvents):
	jsonSchema = spark.read.json(trainEvents.rdd.map(lambda r: r.payload)).schema
	
	events = trainEvents.withColumn('json', from_json(col('payload'), jsonSchema)).select("station_id", "ris_status_id", "timetable_id", "json")
	events = events.select(events.station_id, 
				events.ris_status_id, 
				events.timetable_id, 
				events.json.info.event_datetime.alias("datetime"), 			
				events.json.info.train_id.alias("train_id"))
	
	events.registerTempTable("events")
	ris.registerTempTable("ris")
	stations.registerTempTable("stations")
	timetable.registerTempTable("timetable")
	
	stationdata = spark.sql("SELECT e.station_id, e.ris_status_id, e.timetable_id, e.datetime, e.train_id, r.name, s.longitude, s.latitude, t.station_order FROM events AS e, ris AS r, stations AS s, timetable as t WHERE e.ris_status_id = r.id and e.station_id = s.id and e.station_id = t.station_id and e.timetable_id = t.timetable_id order by datetime")
	stationdata.registerTempTable("stationdata")
	
	spark.sql("select l.station_id, dt_start, dt_end from (select datetime as dt_start, train_id, station_id from stationdata where name ='Arrival' or name = 'TakenOver') as l, (select datetime as dt_end, train_id, station_id from stationdata where name ='Departure' or name = 'HandedOver') as r where l.station_id = r.station_id and l.train_id = r.train_id").registerTempTable("station_events")
	train_station_data_init = spark.sql("select station_id, hour(dt_start) as hour, dayofmonth(dt_start) AS day, month(dt_start) as month, unix_timestamp(to_utc_timestamp(dt_end, 'GMT')) - unix_timestamp(to_utc_timestamp(dt_start, 'GMT')) as dur_stop from station_events")
	train_station_data_init.registerTempTable("train_station_data")
	
	train_station_data = spark.sql("select enc.* from (select station_id, percentile_approx(dur_stop, 0.2, 1e9) as q25, percentile_approx(dur_stop, 0.8, 1e9) as q75 from train_station_data where dur_stop > 0 group by station_id) as percs, train_station_data as enc where percs.station_id == enc.station_id and enc.dur_stop > percs.q25 and enc.dur_stop < q75") 

	train_station_data.registerTempTable("train_station_data2")

	median_per_station = spark.sql("select station_id, percentile_approx(dur_stop, 0.5, 1e9) as median from train_station_data2 group by station_id")

	feature_columns = ["station_id", "hour", "day", "month"]
	indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").setHandleInvalid("skip").fit(train_station_data) for column in feature_columns]
	pipeline = Pipeline(stages=indexers)
	pipelineIndexerModel = pipeline.fit(train_station_data)
	train_station_data = pipelineIndexerModel.transform(train_station_data)

	encoderModel = OneHotEncoderEstimator(dropLast=False, inputCols=["station_id_index", "hour_index", "day_index", "month_index"], outputCols=["station_id_ohe", "hour_ohe", "day_ohe", "month_ohe"]).fit(train_station_data)
	encoded_train_station_data = encoderModel.transform(train_station_data)

	station_name_dict = {}	
	for colname in feature_columns:
		colIdx = encoded_train_station_data.select(colname, colname + "_index").distinct().rdd.collectAsMap()
		colIdx =  sorted((value, colname + "_" + str(key)) for (key, value) in colIdx.items())
		station_name_dict[colname] = colIdx
		newCols = list(map(lambda x: str(x[1]).split(".")[0], colIdx))
		actualCol = encoded_train_station_data.columns
		allColNames = actualCol + newCols
		encoded_train_station_data = encoded_train_station_data.rdd.map(lambda x : extract(x, colname)).toDF(allColNames)

	final_columns = [c for c in encoded_train_station_data.columns \
						if c not in set(feature_columns) and \
							c not in set(list(map(lambda x : x + "_index", feature_columns))) and \
							c not in set(list(map(lambda x : x + "_ohe", feature_columns)))] 

	encoded_train_station_data = encoded_train_station_data[final_columns]
							
	assembler = VectorAssembler(inputCols=encoded_train_station_data.columns, outputCol="data")
	corr_dataframe = assembler.transform(encoded_train_station_data).select("data")

	corr_mat = Correlation.corr(corr_dataframe, "data").collect()[0]["pearson(data)"].values
	corr_mat = np.reshape(corr_mat, (len(encoded_train_station_data.columns), -1))
	corr_mat = corr_mat[0, :]
	train_data_columns = np.array(encoded_train_station_data.columns)[np.abs(corr_mat) >= 0.05].tolist()
	train_data = encoded_train_station_data[train_data_columns]

	trainAssembler = VectorAssembler(inputCols = [c for c in train_data.columns if c != "dur_stop"], outputCol = 'features')
	vtrain_data = trainAssembler.transform(train_data)
	vtrain_data = vtrain_data.select(['features', 'dur_stop'])

	lr = LinearRegression(featuresCol = 'features', labelCol='dur_stop')
	lr_station_model = lr.fit(vtrain_data)

	return_dict = {}
	return_dict["stationNameDict"] = station_name_dict
	return_dict["pipelineIndexerModel"] = pipelineIndexerModel
	return_dict["oheStationModel"] = encoderModel
	return_dict["allStationColumns"] = final_columns
	return_dict["keepStationColumns"] = train_data_columns
	return_dict["stationTrainAssembler"] = trainAssembler
	return_dict["stationTrainModel"] = lr_station_model
	return_dict["medianTimePerStation"] = median_per_station

	spark.sql("select l.longitude as start_lon, l.latitude as start_lat, r.longitude as end_lon, r.latitude as end_lat, dt_start, dt_end from (select datetime as dt_start, train_id, station_id, station_order, longitude, latitude from stationdata where name ='Departure' or name = 'HandedOver') as l, (select datetime as dt_end, train_id, station_id, station_order, longitude, latitude from stationdata where name ='Arrival' or name = 'TakenOver') as r where l.station_order + 1 == r.station_order and l.train_id = r.train_id").registerTempTable("route_events")

	train_route_data_init = spark.sql("select start_lon, start_lat, end_lon, end_lat, hour(dt_start) as hour, dayofmonth(dt_start) AS day, month(dt_start) as month, unix_timestamp(to_utc_timestamp(dt_end, 'GMT')) - unix_timestamp(to_utc_timestamp(dt_start, 'GMT')) as dur_route from route_events")

	train_route_data_init.registerTempTable("train_route_data")

	train_route_data = spark.sql("select * from train_route_data where dur_route > (select percentile_approx(dur_route, 0.15, 1e9) from train_route_data where dur_route > 0) and dur_route < (select percentile_approx(dur_route, 0.85, 1e9) from train_route_data where dur_route > 0)")

	train_route_data.registerTempTable("temp_route")
	partial_train = spark.sql("select start_lon, start_lat, end_lon, end_lat, dur_route from temp_route")

	feature_columns = ["hour", "day", "month"]

	train_route_data = pipelineIndexerModel.transform(train_route_data)

	encoderRouteModel = OneHotEncoderEstimator(dropLast=False, inputCols=[c + "_index" for c in feature_columns], outputCols=[c + "_ohe" for c in feature_columns]).fit(train_route_data)
	encoded_train_route_data = encoderRouteModel.transform(train_route_data)
	route_name_dict = {}
	for colname in feature_columns:
		colIdx = encoded_train_route_data.select(colname, colname + "_index").distinct().rdd.collectAsMap()
		colIdx =  sorted((value, colname + "_" + str(key)) for (key, value) in colIdx.items())
		route_name_dict[colname] = colIdx
		newCols = list(map(lambda x: str(x[1]).split(".")[0], colIdx))
		actualCol = encoded_train_route_data.columns
		allColNames = actualCol + newCols
		encoded_train_route_data = encoded_train_route_data.rdd.map(lambda x : extract(x, colname)).toDF(allColNames)

	final_columns = [c for c in encoded_train_route_data.columns \
						if c not in set(feature_columns) and \
							c not in set(list(map(lambda x : x + "_index", feature_columns))) and \
							c not in set(list(map(lambda x : x + "_ohe", feature_columns)))]

	order = ["dur_route"]
	order.extend([c for c in final_columns if c != "dur_route"])
	encoded_train_route_data = encoded_train_route_data[order]

	assembler = VectorAssembler(inputCols=encoded_train_route_data.columns, outputCol="data")
	corr_dataframe = assembler.transform(encoded_train_route_data).select("data")

	corr_mat = Correlation.corr(corr_dataframe, "data").collect()[0]["pearson(data)"].values
	corr_mat = np.reshape(corr_mat, (len(encoded_train_route_data.columns), -1))
	corr_mat = corr_mat[0, :]
	route_train_data_columns = np.array(encoded_train_route_data.columns)[np.abs(corr_mat) >= 0.05].tolist()

	route_train_data = encoded_train_route_data[route_train_data_columns]

	trainRouteAssembler = VectorAssembler(inputCols = [c for c in route_train_data.columns if c != "dur_route"], outputCol = 'features')
	vtrain_data = trainRouteAssembler.transform(route_train_data)
	vtrain_data = vtrain_data.select(['features', 'dur_route'])

	gbt = GBTRegressor(featuresCol = 'features', labelCol='dur_route')
	vtrain_data.cache()
	gbt_route_model = gbt.fit(vtrain_data)

	partialTrainRouteAssembler = VectorAssembler(inputCols = ["start_lon", "start_lat", "end_lon", "end_lat"], outputCol = 'features')
	vpart_train = partialTrainRouteAssembler.transform(partial_train)
	vpart_train = vpart_train.select(['features', 'dur_route'])
	
	gbt_partial = GBTRegressor(featuresCol = 'features', labelCol='dur_route')
	vpart_train.cache()
	gbt_partial_model = gbt_partial.fit(vpart_train)

	return_dict["routeNameDict"] = route_name_dict
	return_dict["oheRouteModel"] = encoderRouteModel
	return_dict["allRouteColumns"] = final_columns
	return_dict["keepRouteColumns"] = route_train_data_columns
	return_dict["routeTrainAssembler"] = trainRouteAssembler
	return_dict["routeTrainModel"] = gbt_route_model
	return_dict["routePartialTrainAssembler"] = partialTrainRouteAssembler
	return_dict["routePartialTrainModel"] = gbt_partial_model

	return(return_dict)
