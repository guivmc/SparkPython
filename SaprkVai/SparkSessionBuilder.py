from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import os
import random

class SparkSessionBuilder:

    sparkSession = ''
    schema = ''

    def __init__(self):
        #conf = pyspark.SparkConf().setAll([("spark.speculation", "false"), ('spark.network.timeout', '600s')])
        # self.sparkSession = SparkSession.builder.config('spark.network.timeout', '6000s').getOrCreate()
        self.sparkSession = SparkSession.builder.config('spark.network.timeout', '6000s').getOrCreate()
        self.schema = StructType([
            StructField("StringEstatica", StringType(), True),
            StructField("NumAleatorio", IntegerType(), True)])

    def csv_to_SparkDataFrame(self):
        # lines = self.sparkSession.sparkContext.textFile(os.path.abspath("CSV/Test.csv"))
        # parts = lines.map(lambda l: l.split(";"))
        # data = parts.map(lambda p: (p[0], p[1].strip()))
        #
        # dataFrame = self.sparkSession.createDataFrame(data, schema=self.schema)

        dataFrame = self.sparkSession.read.csv(os.path.abspath("CSV/Test.csv"), schema=self.schema, sep=";")
        dataFrame.show()
        dataFrame.write.mode('overwrite').format('parquet').parquet(os.path.abspath("Parquet/Test.parquet"))

    def generateRandomParquet(self, parquetSize=2):
        staticString = "AAAAAA"

        nums = []

        row = Row("StringEstatica", "NumAleatorio")

        row1 = row(staticString, random.randint(1, 101))
        row2 = row(staticString, random.randint(1, 101))
        row3 = row(staticString, random.randint(1, 101))

        # for x in range(parquetSize):
        #     rand = random.randint(1, 101)
        #     #nums.append([staticString, rand])
        #     nums.append(row(staticString, rand))

        dataFrame = self.sparkSession.createDataFrame([row1, row2, row3])
        dataFrame.show()
        dataFrame.write.mode('overwrite').format('parquet').parquet(os.path.abspath("Rand/TestRandom.parquet"))

    def sumRandomParquet(self):
        dataFrame = self.sparkSession.read.load(os.path.abspath("Parquet/Test.parquet"))

        dataFrame.createOrReplaceTempView("myView")

        result = self.sparkSession.sql("Select sum(NumAleatorio) From myView")

        result.show()