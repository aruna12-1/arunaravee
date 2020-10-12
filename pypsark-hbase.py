from pyspark.sql import SparkSession

from pyspark.sql import functions as f

from pyspark.sql.types import StringType

spark=SparkSession.builder.master('local').appName('spark-hbase integration').getOrCreate()

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load('/user/arunasai14/traffic_data.csv')

df2 = df.withColumn("Master Record Number",f.trim(df["Master Record Number"].cast(StringType())))\
    .withColumn("Year",f.trim(df["Year"].cast(StringType())))\
    .withColumn("Month",f.trim(df["Month"].cast(StringType())))\
    .withColumn("Hour",f.trim(df["Hour"].cast(StringType())))\
    .withColumn("Day",f.trim(df["Day"].cast(StringType())))\
    .withColumn("Latitude",f.trim(df["Latitude"].cast(StringType())))\
    .withColumn("Longitude",f.trim(df["Longitude"].cast(StringType())))


dataSourceFormat = "org.apache.spark.sql.execution.datasources.hbase"

writeCatalog =("""  {"table":{"namespace":"traf_data", "name":"traf_tbl"},\
        "rowkey":"key",\
        "columns":{\
          "Master Record Number":{"cf":"rowkey", "col":"key", "type":"string"},\
          "Year":{"cf":"traf_dtls", "col":"Year", "type":"string"},\
          "Month":{"cf":"traf_dtls", "col":"Month", "type":"string"},\
          "Day":{"cf":"traf_dtls", "col":"Day", "type":"string"},\
          "Weekend?":{"cf":"traf_dtls", "col":"Weekend", "type":"string"},\
          "Hour":{"cf":"traf_dtls", "col":"Hour", "type":"string"},\
          "Collision Type":{"cf":"traf_dtls", "col":"Collsn_Type", "type":"string"},\
          "Injury Type":{"cf":"other_info", "col":"Injury_Type", "type":"string"},\
          "Primary Factor":{"cf":"other_info", "col":"Primary_Factor", "type":"string"},
                  "Reported_Location":{"cf":"other_info", "col":"Rpted_Loc", "type":"string"},\
          "Latitude":{"cf":"other_info", "col":"Latitude", "type":"string"},
                  "Longitude":{"cf":"other_info", "col":"Longitude", "type":"string"}
          
        }
              } 
""")

readCatalog =("""  {"table":{"namespace":"traf_data", "name":"traf_tbl"},\
        "rowkey":"key",\
        "columns":{\
          "Master Record Number":{"cf":"rowkey", "col":"key", "type":"string"},\
          "Year":{"cf":"traf_dtls", "col":"Year", "type":"string"},\
          "Month":{"cf":"traf_dtls", "col":"Month", "type":"string"},\
          "Day":{"cf":"traf_dtls", "col":"Day", "type":"string"},\
          "Weekend?":{"cf":"traf_dtls", "col":"Weekend", "type":"string"},\
          "Hour":{"cf":"traf_dtls", "col":"Hour", "type":"string"},\
          "Collision Type":{"cf":"traf_dtls", "col":"Collsn_Type", "type":"string"},\
          "Injury Type":{"cf":"other_info", "col":"Injury_Type", "type":"string"},\
          "Primary Factor":{"cf":"other_info", "col":"Primary_Factor", "type":"string"},
                  "Reported_Location":{"cf":"other_info", "col":"Rpted_Loc", "type":"string"},\
          "Latitude":{"cf":"other_info", "col":"Latitude", "type":"string"},
                  "Longitude":{"cf":"other_info", "col":"Longitude", "type":"string"}
          
        }
              } 
""")

#print("csv file read", df2.show())
##df2.write.options(catalog=writeCatalog, newtable=5).format(dataSourceFormat).save()

readDF = spark.read.options(catalog=readCatalog).format(dataSourceFormat).load()


readDF.show()