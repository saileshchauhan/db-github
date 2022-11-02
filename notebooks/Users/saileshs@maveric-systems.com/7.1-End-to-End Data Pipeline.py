# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions  import from_unixtime
from pyspark.sql.functions  import to_date
from pyspark.sql import Row
from pyspark.sql.functions import to_json, struct
from pyspark.sql import functions as F
import random
import time

# COMMAND ----------

# Config details for Azure SQL DB for VehicleInformation and LocationInformation tables
#sqldbusername = dbutils.secrets.get(scope="KeyVaultScope",key="VehicleInformationDBUserId")
#sqldbpwd=dbutils.secrets.get(scope="KeyVaultScope",key="VehicleInformationDBPwd")
sqldbusername="dataadmin"
sqldbpwd="admin@123"
jdbcHostname = "sql-server-adb-usecase.database.windows.net"
jdbcDatabase = "sql-database"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, sqldbusername, sqldbpwd)
connectionProperties = {
  "user" : sqldbusername,
  "password" : sqldbpwd,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Reading dbo.VehicleInfo master table from Azure SQL DB
vehicleInfo = "(select VehicleId,Make,Model,Category,ModelYear from dbo.VehicleInformation) vehilce"
df_vehicleInfo = spark.read.jdbc(url=jdbcUrl, table=vehicleInfo, properties=connectionProperties)
display(df_vehicleInfo)

# COMMAND ----------

df_vehicleInfo.createOrReplaceTempView("vw_VehicleMaster")

# COMMAND ----------

# Reading dbo.LocationInfo master table from Azure SQL DB and creating a view

locationInfo = "(select Borough,Location,Latitude,Longitude from dbo.LocationInfo) vehilce"
df_locationInfo = spark.read.jdbc(url=jdbcUrl, table=locationInfo, properties=connectionProperties)

df_locationInfo.createOrReplaceTempView("vw_LocationMaster")
display(df_locationInfo)

# COMMAND ----------

storageAccount="stdev8085"
mountpoint = "/mnt/SensorData"
storageEndPoint ="abfss://sensordata@{}.dfs.core.windows.net/".format(storageAccount)
print ('Mount Point ='+mountpoint)

#ClientId, TenantId and Secret is for the Application(ADLSGen2App) was have created as part of this recipe
clientID ="81f6d479-4da2-47cd-ae39-3690c03c1de6"
tenantID ="26038e67-6fc0-4db3-9292-c9804fc51539"
clientSecret ="LL.8Q~kPdp9hagY0JCKHAkG5NlFi5YtsrO4MSaTI"
oauth2Endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(tenantID)


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": oauth2Endpoint}

try:
  dbutils.fs.mount(
  source = storageEndPoint,
  mount_point = mountpoint,
  extra_configs = configs)
except:
    print("Already mounted...."+mountpoint)


# COMMAND ----------

# dbutils.fs.unmount("/mnt/SensorData")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/SensorData//"))

# COMMAND ----------

#Creating the schema for the vehicle data json structure
jsonschema = StructType() \
.add("id", StringType()) \
.add("eventtime", TimestampType()) \
.add("rpm", IntegerType()) \
.add("speed", IntegerType()) \
.add("kms", IntegerType()) \
.add("lfi", IntegerType())  \
.add("lat", DoubleType()) \
.add("long", DoubleType())

# COMMAND ----------

#Function to create required folders in mount point
def checkpoint_dir(type="Bronze"): 
  val = f"/mnt/SensorData/vehiclestreamingdata/{type}/chkpnt/" 
  return val

def delta_dir(type="Bronze"): 
  val = f"/mnt/SensorData/vehiclestreamingdata/{type}/delta/" 
  return val

def hist_chkpt_dir(type="Hist"): 
  val = f"/mnt/SensorData/historicalvehicledata/{type}/chkpnt" 
  return val
 
def hist_dir(type="Hist"): 
  val = f"/mnt/SensorData/historicalvehicledata/{type}/data" 
  return val
 

# COMMAND ----------

#Event Hubs for Kafak configuration details
BOOTSTRAP_SERVERS = "kafkaenabledeventhubns.servicebus.windows.net:9093"
EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://eventhubns.servicebus.windows.net/;SharedAccessKeyName=sendreceivekafka;SharedAccessKey=4adasdasda+SFHpRyQasdasdasd=\";"
GROUP_ID = "$Default"


# COMMAND ----------

# Function to read data from EventHub and writing as delta format
def append_kafkadata_stream(topic="vehiclesensoreventhub"):
  kafkaDF = (spark.readStream \
    .format("kafka") \
    .option("subscribe", topic) \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", EH_SASL) \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.session.timeout.ms", "60000") \
    .option("kafka.group.id", GROUP_ID) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
    .load().withColumn("source", lit(topic)))
  
  newkafkaDF=kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","source").withColumn('vehiclejson', from_json(col('value'),schema=jsonschema))
  kafkajsonDF=newkafkaDF.select("key","value","source", "vehiclejson.*")
  
  query=kafkajsonDF.selectExpr(
                  "id"	  \
                  ,"eventtime"	   \
                  ,"rpm"	\
                  ,"speed" \
                  ,"kms" \
                  ,"lfi" \
                  ,"lat" \
                  ,"long" \
                  ,"source") \
            .writeStream.format("delta") \
            .outputMode("append") \
            .option("checkpointLocation",checkpoint_dir("Bronze")) \
            .start(delta_dir("Bronze")) 
 
  return query

# COMMAND ----------

# Function to read data from ADLS gen-2 using readStream API and writing as delta format
def append_batch_source():
  
  topic ="historical"

  kafkaDF = (spark.readStream \
     .schema(jsonschema)
    .format("parquet") \
    .load(hist_dir("Hist")).withColumn("source", lit(topic)))
  
  query=kafkaDF.selectExpr(
                  "id"	  \
                  ,"eventtime"	   \
                  ,"rpm"	\
                  ,"speed" \
                  ,"kms" \
                  ,"lfi" \
                  ,"lat" \
                  ,"long" \
                  ,"source"
                 ) \
            .writeStream.format("delta") \
            .option("checkpointLocation",checkpoint_dir("Hist")) \
            .outputMode("append") \
            .start(delta_dir("Hist")) 
 
  return query

# COMMAND ----------

# Reading data from EventHubs for Kafka
query_source1 = append_kafkadata_stream(topic='vehiclesensoreventhub')


# COMMAND ----------

# Reading data from Historical location ( in this example its from ADLS Gen-2 having historical data for Vehicle Sensor.)
# There may be cases where historical data can be added to this location from any other source where the schema is same for all the files. IN such scenarios using readStream API on Gen-2 location will keep polling for new data and when available it will be ingested
query_source2 = append_batch_source()

# COMMAND ----------

# Dropping all Delta tables if required
def DropDeltaTables(confirm=1):
  
  if(confirm ==1):
    spark.sql("DROP TABLE IF EXISTS VehicleSensor.VehicleDelta_Bronze")
    spark.sql("DROP TABLE IF EXISTS VehicleSensor.VehicleDelta_Silver")
    spark.sql("DROP TABLE IF EXISTS VehicleSensor.VehicleDeltaAggregated")
    spark.sql("DROP TABLE IF EXISTS VehicleSensor.VehicleDelta_Historical")
    

# COMMAND ----------

#Function which drops all delta tables. TO avoid droping tables call the function with confirm=0
DropDeltaTables(confirm=0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the delta table on delta location for Bronze data
# MAGIC CREATE DATABASE IF NOT EXISTS VehicleSensor;

# COMMAND ----------

#Create historical delta table
spark.sql("CREATE TABLE IF NOT EXISTS VehicleSensor.VehicleDelta_Historical USING DELTA LOCATION '{}'".format(delta_dir("Hist")))

# COMMAND ----------

# Wait for 5 seconds before we create the delta tables else it might error out that delta location is not created
time.sleep(5) 

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Creating Bronze Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the delta table on delta location for Bronze data
# MAGIC CREATE DATABASE IF NOT EXISTS VehicleSensor;
# MAGIC CREATE TABLE IF NOT EXISTS VehicleSensor.VehicleDelta_Bronze
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/mnt/SensorData/vehiclestreamingdata/Bronze/delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted VehicleSensor.VehicleDelta_Bronze

# COMMAND ----------

#Streaming Data from Bronze Delta Table. This will help in only extracting new data coming from Event Hubs to be loaded into Silver Delta tables.
df_bronze=spark.readStream.format("delta").option("latestFirst", "true").table("VehicleSensor.VehicleDelta_Bronze")

# COMMAND ----------

#Creating Temp View on Bronze DF
df_bronze.createOrReplaceTempView("vw_TempBronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vw_TempBronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*),hour(eventtime) as hour, day(eventtime) as day from vw_TempSilver group by hour(eventtime),day(eventtime)
# MAGIC select *, Year(eventtime) as Year, month(eventtime) as Month,day(eventtime) as Day, hour(eventtime) as Hour from vw_TempBronze limit 10

# COMMAND ----------

#Streaming Data from History Delta Table
df_historical=spark.readStream.format("delta").option("latestFirst", "true").table("VehicleSensor.VehicleDelta_Historical")

# COMMAND ----------

#Joining both historical and Bronze Streaming Data
df_bronze_hist = df_bronze.union(df_historical)

# COMMAND ----------

df_bronze_hist.createOrReplaceTempView("vw_TempBronzeHistorical")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vw_TempBronzeHistorical

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Creating Silver Delta Table

# COMMAND ----------

# We are always combining data from streaming and batch data by using Structured streaming

df_silver= spark.sql("select s.*,m.Make,m.Model,m.Category, Year(eventtime) as Year, month(eventtime) as Month,day(eventtime) as Day, \
                     hour(eventtime) as Hour,l.Borough,l.Location  \
                     from vw_TempBronzeHistorical s \
                     left join vw_VehicleMaster m on s.id = m.VehicleId \
                     left join vw_LocationMaster l on s.lat = l.Latitude and s.long = l.Longitude") \
            .writeStream.format("delta").option("MergeSchem","True") \
            .outputMode("append") \
            .option("checkpointLocation",checkpoint_dir("Silver"))  \
            .start(delta_dir("Silver"))

# COMMAND ----------

# Wait for 3 seconds before we create the delta tables else it might error out that delta location is not created
time.sleep(5) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop  TABLE IF  EXISTS VehicleSensor.VehicleDelta_Silver;
# MAGIC CREATE TABLE IF NOT EXISTS VehicleSensor.VehicleDelta_Silver
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/mnt/SensorData/vehiclestreamingdata/Silver/delta/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from VehicleSensor.VehicleDelta_Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted VehicleSensor.VehicleDelta_Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Creating Gold Delta Table

# COMMAND ----------


df_gold=(spark.readStream.format("delta").option("latestFirst", "true").table("VehicleSensor.VehicleDelta_Silver") \
                                 .groupBy(window('eventtime',"1 hour"),"Make","Borough","Location","Month","Day","Hour").count()) \
                                 .writeStream.format("delta") \
                                              .outputMode("complete") \
                                              .option("checkpointLocation",checkpoint_dir("Gold"))  \
                                              .start(delta_dir("Gold"))


# COMMAND ----------

time.sleep(10) 

# COMMAND ----------

#Create VehicleDeltaAggregated delta table

spark.sql("CREATE TABLE IF NOT EXISTS VehicleSensor.VehicleDeltaAggregated USING DELTA LOCATION '{}'".format(delta_dir("Gold")))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select max(eventtime) from VehicleSensor.VehicleDelta_Silver where eventtime between '2021-05-06T16:00:46.905+0000' and '2021-05-06T17:00:00.905+0000'--id='aa7e09d0-92cb-4cc0-99e2-04602ab0f85a'  order by Eventtime desc limit 10;
# MAGIC 
# MAGIC -- select * from  VehicleSensor.VehicleDelta_Bronze  where id='aa7e09d0-92cb-4cc0-99e2-04602ab0f85a' order by Eventtime desc limit 10;
# MAGIC 
# MAGIC select * from  VehicleSensor.VehicleDelta_Silver  where id='aa7e09d0-92cb-4cc0-99e2-04602ab0f85a' order by Eventtime desc limit 10;

# COMMAND ----------

df_silver_agg=(spark.readStream.format("delta").table("VehicleSensor.VehicleDelta_Silver"))
df_silver_agg.createOrReplaceTempView("vw_AggDetails")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Viwing data from the Gold Delta Tables
# MAGIC select * from VehicleSensor.VehicleDeltaAggregated
# MAGIC ORDER BY Month DESC, Day Desc,count desc  limit 10

# COMMAND ----------

# server_name = "vehicleinformatiosrvr.database.windows.net"
# database_name = "VehicleInformationDB"
# url = server_name + ";" + "databaseName=" + database_name + ";"

# table_name = "VehicleInformation"
# username = sqldbusernamedd
# password = sqldbpwd
# jdbcDF = spark.read \
#         .format("com.microsoft.sqlserver.jdbc.spark") \
#         .option("url", url) \
#         .option("dbtable", table_name) \
#         .option("user", username) \
#         .option("password", password).load()

# COMMAND ----------

# jdbcDF.load()

# COMMAND ----------

writeConfig = {
    "Endpoint": "https://test.documents.azure.com:443/",
    "Masterkey": "",
    "Database": "vehicle",
    "Collection": "vehicleinformationtest",
    "Upsert": "true",
    "WritingBatchSize": "500"
   }