
from pyspark.sql import SparkSession


spark=sparksession.builder.master('local').appName('spark-avro').getOrCreate()

df=spark.read.format('com.databricks.spark.avro').load('/user/arunasai14/userdata1.avro')

df.show()


step 1:

hadoop fs -put /home/arunasai14/dataset/userdata1.avro /user/arunasai14/



spark2-submit --master local --conf spark.ui.port=0  --packages com.databricks:spark-avro_2.11:4.0.0 /home/arunasai14/avropy.py