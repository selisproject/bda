import json

def run(spark, message_dataframe, ints):
	message = message_dataframe.collect()[0]
	theId = message['id']
	other = message['payload']

	#adict = json.loads(other)

	rowprod = ints.where(ints["id"] == theId).select((ints["int1"] * ints["int2"]).alias("sum")).collect()[0]["sum"]

	print(rowprod)
	result_dict = {}
	result_dict["number"] = rowprod
	result_dict["other"] = other

	return result_dict


