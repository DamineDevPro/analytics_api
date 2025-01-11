from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import os

MONGO_URL = "mongodb+srv://root_DB:euVZCp4CEMnSkkap@delivxecomm-xtyzm.mongodb.net/delivxEcomm?retryWrites=true&w=majority"
MONGO_DB = "delivxEcomm"

# MONGO_DB = str(os.getenv("MONGO_DB"))
SPARK_HOST = "localhost"
# MONGO_URL = os.getenv("MONGODB_URL")

conf = SparkConf().set("spark.driver.host", SPARK_HOST)
conf.set("spark.jars.packages",
         "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
conf.set('spark.mongodb.input.uri', MONGO_URL)
conf.set('spark.mongodb.output.uri', MONGO_URL)


sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)


df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource"). \
    option("database", MONGO_DB). \
    option("collection", "storeOrder"). \
    option('sampleSize', 100000).load()

# df_store = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource"). \
#     option("database", MONGO_DB). \
#     option("collection", "stores"). \
#     option('sampleSize', 100000).load()

storeOrder = sqlContext.sql("SELECT _id, cartId, status FROM storeOrder")
# stores = sqlContext.sql("SELECT * FROM storeOrder")
storeOrder.show()
# stores = stores.toPandas()
# print(storeOrder)


