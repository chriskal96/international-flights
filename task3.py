import findspark
findspark.init()
from pyspark.sql import SparkSession
import pandas as pd

MAX_MEMORY = "14g"

spark = SparkSession \
    .builder \
    .appName("testFlight") \
    .config("spark.executor.memory", MAX_MEMORY) \
    .config("spark.driver.memory", MAX_MEMORY) \
    .getOrCreate()
	
#enter path of file here
path = "C:/Users/Christos Kallaras/Desktop/Master/1st Year/2nd Trimester/Big Data Systems and Architectures/Spark/Assignment"
flights = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv( path + "/671009038_T_ONTIME_REPORTING.csv")

flights.show(5)

#show the colums names
flights.printSchema()

#convert the dataframe into a table
flights.createOrReplaceTempView("flights")

airports = spark.sql("""
SELECT ORIGIN,count(*) AS FLIGHTS
FROM flights
GROUP BY ORIGIN
ORDER BY FLIGHTS DESC
""")
airports.show(5)

airways = spark.sql("""
SELECT CARRIER,count(*) AS FLIGHTS
FROM flights
GROUP BY CARRIER
ORDER BY FLIGHTS DESC
""")
airways.show(5)

#convert the dataframe into a table
airports.createOrReplaceTempView("airports")

#we want the result in a variable so we can use it for the next queries so we retuen the first value (since the query will return one value)
limit_airports = spark.sql("""
SELECT percentile(FLIGHTS, 0.1) AS limit FROM airports 
""").first()["limit"]
limit_airports

#convert the dataframe into a table
airways.createOrReplaceTempView("airways")

limit_airways = spark.sql("""
SELECT percentile(FLIGHTS, 0.1) AS limit FROM airways 
""").first()["limit"]
limit_airways

#construt querry
query = """
SELECT flights.* FROM flights, 
(
select CARRIER,COUNT (*) as number_flights
from flights 
group by CARRIER
) AS A ,
(
select ORIGIN,COUNT (*) as number_flights
from flights 
group by ORIGIN
) AS B
WHERE flights.CARRIER = A.CARRIER
AND flights.ORIGIN = B.ORIGIN   
AND A.number_flights > """ + str(limit_airways) + """ 
AND B.number_flights > """ + str(limit_airports)  

query

data_without_outliers = spark.sql(query)
data_without_outliers.show(5)

from pyspark.sql.functions import date_format, col
prepped_dataframe = flights\
.na.fill(0)\
.withColumn("hour_of_day", (col("DEP_TIME")/100).cast('int'))
prepped_dataframe.select("hour_of_day").distinct().orderBy("hour_of_day").show()

from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

indexer1 = StringIndexer()\
.setInputCol("ORIGIN")\
.setOutputCol("origin_index")

indexer2 = StringIndexer()\
.setInputCol("CARRIER")\
.setOutputCol("carrier_index")

indexer3 = StringIndexer()\
.setInputCol("hour_of_day")\
.setOutputCol("hour_of_day_index")


encoder = OneHotEncoder(dropLast=False)\
.setInputCols(["origin_index","carrier_index","hour_of_day_index"])\
.setOutputCols(["origin_encoded","carrier_encoded","hour_of_day_encoded"])

vector_assembler = VectorAssembler()\
.setInputCols(["origin_encoded","carrier_encoded","hour_of_day_encoded"])\
.setOutputCol("features")

transformation_pipeline = Pipeline()\
.setStages([indexer1,indexer2,indexer3, encoder, vector_assembler])

fitted_pipeline = transformation_pipeline.fit(prepped_dataframe)

transformed_dataframe = fitted_pipeline.transform(prepped_dataframe)
transformed_dataframe.cache()
transformed_dataframe.show(5)


splits = transformed_dataframe.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='DEP_DELAY', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)

print("R2 = " + str(lr_model.summary.r2))

#make the predictions
lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","DEP_DELAY","features").show(10)

from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="DEP_DELAY",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

#find the Root Mean Squared Error (RMSE)
test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) = " + str(test_result.rootMeanSquaredError))


spark.stop()


