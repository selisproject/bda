from pyspark.ml.regression import LinearRegression

def run(spark, training_data, maxIter, regParam, elasticNetParam):
	lr = LinearRegression(maxIter, regParam, elasticNetParam)

	# Fit the model
	lrModel = lr.fit(training_data)

	# Summarize the model over the training set and print out some metrics
	trainingSummary = lrModel.summary
	print("numIterations: %d" % trainingSummary.totalIterations)
	print("RMSE: %f" % trainingSummary.rootMeanSquaredError)

	return lrModel