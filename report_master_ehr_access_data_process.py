import sys
import boto3
import pandas as pd
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

today = datetime.datetime.utcnow().date()

ehr_access_df = spark.read.\
           format("parquet").\
           load("s3://axai//Analytics_Data_Lake/SpreadSheet_Data/Master_EHR_Access_Data"+"/*")
           
ehr_access_df = ehr_access_df.replace('None', None)
ehr_access_df.createOrReplaceTempView("master_ehr_access_info")

report_query="select id,first_name,last_name,date_of_birth as dob,scribe_email,location,ehr,account_name,clinician_name,ticket_status,ticket_no from master_ehr_access_info"

df_f = spark.sql(report_query)


print(df_f.toPandas().head(10))
#pre_query='truncate table view_ImpDailyReportv4_v1;'
#dynamic_frame = DynamicFrame.fromDF(df_f, glueContext,"battery_df")

'''datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_frame, catalog_connection = "rds_sqlserver", connection_options = {"dbtable": "view_ImpDailyReportv4_v1", "database": "aug_bi_dw",'preactions': pre_query}, transformation_ctx = "datasink4")'''

df_f.write.option("truncate", "true") \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://axaidbinstance.ch2izlk4w3mm.us-west-1.rds.amazonaws.com:49670;databaseName=aug_bi_dw") \
    .option("dbtable", "import_master_ehr_access") \
    .option("user", "etl_user") \
    .option("password", "a&i@#pro$*2000@") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()
 
job.commit()
