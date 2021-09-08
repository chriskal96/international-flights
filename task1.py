import findspark
findspark.init()

from pyspark.sql import SparkSession

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

#run the sql command
task1_query = spark.sql("""
SELECT AVG(DEP_DELAY + ARR_DELAY + CARRIER_DELAY + CARRIER_DELAY + WEATHER_DELAY + NAS_DELAY + SECURITY_DELAY + LATE_AIRCRAFT_DELAY) AS AVG_DELAY
FROM flights
""")
task1_query.show()

spark.stop()