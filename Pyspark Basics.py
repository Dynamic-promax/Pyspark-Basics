from pyspark.sql import SparkSession

#Create pyspark Session
spark = SparkSession.builder.appName("PySparkBasics").getOrCreate()

#Load CSV File
df = spark.read.csv(r"C:\\Users\\DELL\\Desktop\\Data Engineering NEW\\Data\\CSV\\taxi_zone_lookup.csv", header=True, inferSchema=True)

#Show first 5 rows
df.show(5)
df.printSchema()

#Select specific colmns
df.select("Borough", "Zone").show()

##Filter data (show only Queens Borough)

df.filter(df.Borough == "Queens").show()

#Rename a column
df = df.withColumnRenamed("Borough", "District")
#Show first 5 rows
df.show(5)
df.printSchema()

#Optimize Spark Queries
from pyspark.sql.functions import broadcast
df = df.repartition("Zone")
df.cache()
Zone_info = spark.read.csv(r"C:\\Users\\DELL\\Desktop\\Data Engineering NEW\\Data\\CSV\\taxi_zone_lookup.csv", header=True, inferSchema=True)
df = df.join(broadcast(Zone_info), "Zone", "left")
df.show(5)

#Save Processed Data
df.write.mode("overwrite").parquet("C:/Users/DELL/Desktop/Data Engineering NEW/NEW Python Codes/output")

#Run SQL QUERIES
df.createOrReplaceTempView("Zone_info") #Create a temporary table
spark.sql("SELECT District, Zone FROM Zone_info WHERE Zone = Astoria").show()
df.show(10)

#Create RDD
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\DELL\\AppData\\Local\\Microsoft\\WindowsApps\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\DELL\\AppData\\Local\\Microsoft\\WindowsApps\\python.exe'
rdd = spark.sparkContext.parallelize([("EWR", "Newark Airport"), ("Queens", "Jamaica Bay")])

#Convert RDD to DataFrame
df_from_rdd = rdd.toDF(["District", "Zone"])
df_from_rdd.show()

#Convert DataFrame back to RDD
rdd_from_df = df_from_rdd
print(rdd_from_df.collect())
