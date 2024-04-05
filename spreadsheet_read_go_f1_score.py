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
df1=obj.read_data("ML Master Note Keys","Master Pivot - By Note ID Dataset Prod Orig Run")

df1.drop(df1.index[0], inplace = True)
df1.columns = df1.iloc[0]
df1.drop(df1.index[0],inplace = True)
df1.dropna(axis=1, how='all',inplace=True)
df1.rename(columns={ df1.columns[0]: "note_id" }, inplace = True)

df1.columns = ["cols_"+str(i) if a == "note_id" else a for i, a in enumerate(df1.columns)]
df1.drop(df1.index[0], inplace = True)
df1=df1[~df1['cols_0'].isnull()]

df1.rename(columns={'cols_0':'note_id','cols_1':'note_section','cols_2':'dataset','True Positive Total':'TP',
                    'False Positive Total':'FP','False Negative Total':'FN',
                    'Accuracy (Precision)':'accuracy_precision','Coverage (Recall)':'coverage_recall'},inplace = True)

df1=df1[['note_id','note_section','dataset','TP','FP','FN','accuracy_precision','coverage_recall','f1']]

df1['TP']=df1['TP'].fillna(0).astype("Int32")
df1['FP']=df1['FP'].fillna(0).astype("Int32")
df1['FN']=df1['FN'].fillna(0).astype("Int32")


df1=obj.replace_none(df1)

df1=df1.astype(str)
sparkDF=spark.createDataFrame(df1) 

sparkDF.write.\
            format("parquet").\
            mode("overwrite").\
            save("s3://axai/Analytics_Data_Lake/SpreadSheet_Data/read_go_f1_score/")

job.commit()

