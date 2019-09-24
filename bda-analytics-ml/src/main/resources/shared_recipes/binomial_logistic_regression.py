from pyspark.ml.classification import LogisticRegression

def run(spark, training_data, maxIter, regParam, elasticNetParam):
	lr = LogisticRegression(maxIter, regParam, elasticNetParam)

	# Fit the model
	lrModel = lr.fit(training_data)
	return lrModel