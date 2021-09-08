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

#convert the dataframe into a table
data_without_outliers.createOrReplaceTempView("data_without_outliers")

#run the sql command
task2_ap_avg = spark.sql("""
SELECT ORIGIN,AVG(DEP_DELAY) AS AVG_DEP_DELAY
FROM data_without_outliers
GROUP BY ORIGIN
ORDER BY AVG(DEP_DELAY) DESC
""")
task2_ap_avg.show(5)

#keep only the top 100
#transform to pandas in order to save to csv
task2_ap_avg.limit(100).toPandas().to_csv('task2-ap-avg.csv',header=False,index=False)

#run the sql command for median of the airports
task2_ap_med = spark.sql("""
SELECT ORIGIN,percentile_approx(DEP_DELAY, 0.5) AS MEDIAN_DEP_DELAY
FROM data_without_outliers
GROUP BY ORIGIN
ORDER BY percentile_approx(DEP_DELAY, 0.5) DESC
""")
task2_ap_med.show(5)

#keep only the top 100
#transform to pandas in order to save to csv
task2_ap_med.limit(100).toPandas().to_csv('task2-ap-med.csv',header=False,index=False)

#run the sql command
task2_aw_avg = spark.sql("""
SELECT CARRIER,AVG(DEP_DELAY) AS AVG_DEP_DELAY
FROM data_without_outliers
GROUP BY CARRIER
ORDER BY AVG(DEP_DELAY) DESC
""")
task2_aw_avg.show(5)

#keep only the top 100
#transform to pandas in order to save to csv
task2_aw_avg.limit(100).toPandas().to_csv('task2-aw-avg.csv',header=False,index=False)

#run the sql command for median of the airways
task2_aw_med = spark.sql("""
SELECT CARRIER,percentile_approx(DEP_DELAY, 0.5) AS MEDIAN_DEP_DELAY
FROM data_without_outliers
GROUP BY CARRIER
ORDER BY percentile_approx(DEP_DELAY, 0.5) DESC
""")
task2_aw_med.show(5)

#keep only the top 100
#transform to pandas in order to save to csv
task2_aw_med.limit(100).toPandas().to_csv('task2-aw-med.csv',header=False,index=False)



#run the sql command for median of the airports
task2_ap_med = spark.sql("""
SELECT ORIGIN, avg(DEP_DELAY) as median
FROM ( SELECT ORIGIN, DEP_DELAY, row, (CASE WHEN column % 2 = 0 then (column DIV 2) ELSE (column DIV 2) + 1 end) as m1, (column DIV 2) + 1 as m2 
        FROM ( 
            SELECT ORIGIN, DEP_DELAY, row_number() OVER (PARTITION BY ORIGIN ORDER BY DEP_DELAY ) as row, count(DEP_DELAY) OVER (PARTITION BY ORIGIN ) as column
            FROM data_without_outliers
         ) s
    ) r
WHERE row BETWEEN m1 and m2
GROUP BY ORIGIN
ORDER BY median  DESC
""")
task2_ap_med.show(5)

spark.stop()