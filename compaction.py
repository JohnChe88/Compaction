import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)

databaseName=""
table_name=""
paritionCols=""
sortCol=""
targetTable=""
s3location=""
rowCountForSize=

# for native spark to access the Glue catalog
spark = SparkSession.builder \
        .appName("Compaction") \
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

spark.catalog.setCurrentDatabase(databaseName)


job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read the S3 folder with Parquet
S3ParquetReader = glueContext.create_dynamic_frame.from_catalog(
    database=databaseName, table_name=table_name, transformation_ctx="S3ParquetReader"
)

#Converting to Native Spark dataframe
df_rep=S3ParquetReader.toDF()

#Repartition them based on a column and then sort withing paritions
repartitionedDYF = df_rep.repartition(paritionCols)\
                         .sortWithinPartitions(sortCol)


#writing to a S3 location and Glue Catalog table
#Bucketing it parittioncol and limiting it by rows
# datafraame is already sorted baesd on the sort col
repartitionedDYF.write\
                .option("header",True)\
                .mode("overwrite")\
                .format("parquet")\
                .option("maxRecordsPerFile", rowCountForSize)\
                .partitionBy(paritionCols)\
                .option("path", s3location)\
                .saveAsTable(targetTable)
