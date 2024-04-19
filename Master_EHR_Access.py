import sys
import pandas as pd
import numpy as np
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from custom_module import SpreadsheetProcess 

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


obj=SpreadsheetProcess.SpreadsheetProcess()

df=obj.read_data("Master EHR Access Table (ALL SITES)","Master")
df=obj.filter_blanks(df,0,'all')
df=obj.replace_none(df)
df=obj.rename_columns(df,{'ID': 'id', 'First Name': 'first_name','Last Name': 'last_name', 
                                'DOB': 'date_of_birth','Scribe Email': 'scribe_email','Location': 'location','EHR': 'ehr',
                                'Account Name': 'account_name','Clinician Name': 'clinician_name','Ticket Status': 'ticket_status',
                                'Ticket #': 'ticket_no'
                                })
df=df.astype(str)
sparkDF=spark.createDataFrame(df) 

sparkDF.write.\
            format("parquet").\
            mode("overwrite").\
            save("s3://axai/Analytics_Data_Lake/SpreadSheet_Data/Master_EHR_Access_Data/")

job.commit()
