import time
import os
import findspark
import glob

import psycopg2
from pyspark.shell import sqlContext

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from config_parser import ConfigParser
from utils.logger_util import get_logger
from utils.postgres_util import GetPostgresConnection

logger = get_logger(__name__)

configs = ConfigParser().config


class LoadJsons:
    def __init__(self, file):
        self.spark_context = SparkSession \
            .builder \
            .appName("PySpark_Testing") \
            .config('spark.jars', configs.get('jar_path')) \
            .getOrCreate()
        logger.info("Spark context created")
        self.filename = file

    def GetDf(self):
        self.df = self.spark_context.read.json(self.filename)
        logger.info("JSON data read from file successful")
        col_names = self.df.columns
        for col_name in col_names:
            cname = col_name.split(".")[-1]
            self.df = self.df.withColumnRenamed(col_name, cname)
        logger.info("Columns renamed successfully")

    def ResolveDatetime(self, datetime_cols):
        for column in datetime_cols:
            self.df = self.df.withColumn(column, col(column).cast(TimestampType()))
        logger.info("Datetime columns datatype resolved successfully")

    def RemoveCorruptValues(self, col_list):
        for c in col_list:
            self.df = self.df.where(col(c).isNotNull())

    def RemoveNegativeValues(self, col_list):
        logger.info(f"Starting to remove negative values. Current row rount = {self.df.count()}")
        self.df = self.df.where("AND".join(["(%s >=0)" % col for col in col_list]))
        logger.info(f"negative values removed. Final row count = {self.df.count()}")

    def WriteDfJDBC(self):
        start_tm = time.time()

        mode = configs.get('mode')
        url = configs.get('url')
        properties = {"user": configs.get('username'),
                      "password": configs.get('password'),
                      "driver": configs.get('driver')}
        tbl_nm = self.filename.split("/")[-1].split(".")[0]
        logger.info(f"Wrting data to {tbl_nm} table with {mode} mode")
        self.df.write.jdbc(url=url, table=tbl_nm, mode=mode, properties=properties)

        end_tm = time.time()
        logger.info(f"Data written successfully to {tbl_nm} in {end_tm - start_tm} seconds using JDBC")

    def WriteDfRepartition(self):
        start_tm = time.time()
        tbl_nm = self.filename.split("/")[-1].split(".")[0]

        self.df.repartition(10).write.format('jdbc').options(
            url=configs.get('url'),
            driver=configs.get('driver'),
            dbtable="{table}".format(table=tbl_nm),
            user=configs.get('username'),
            password=configs.get('password'),
            batchsize=50000,
            queryTimeout=690
        ).mode(configs.get('mode')).save()

        end_tm = time.time()
        logger.info(f"Data written successfully to {tbl_nm} in {end_tm - start_tm} seconds using repartitions JDBC")

    def CreateCSVRepartition(self):
        tbl_nm = self.filename.split("/")[-1].split(".")[0]
        self.df.repartition(10).write.option("delimiter", "\t").format("com.databricks.spark.csv").mode(
            configs.get('mode')).save(f"{configs.get('repartition_data_dir')}{tbl_nm}")

    def WriteDfCOPY(self):
        start_tm = time.time()
        tbl_nm = self.filename.split("/")[-1].split(".")[0]
        files_list = glob.glob(f"{configs.get('repartition_data_dir')}{tbl_nm}/*.csv")

        con = GetPostgresConnection()
        cursor = con.cursor()
        cursor.execute(f"TRUNCATE {tbl_nm};")

        for file in files_list:
            with open(file, 'r') as f:
                try:
                    cursor.copy_from(f, tbl_nm, sep="\t")
                except Exception as e:
                    print(e)

        con.commit()
        con.close()

        end_tm = time.time()
        logger.info(f"Data written successfully in {end_tm - start_tm} seconds using Copy method")
