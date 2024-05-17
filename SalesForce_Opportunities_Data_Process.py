import sys
import pandas as pd
import numpy as np
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from custom_module import SalesForceProcess 

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


obj=SalesForceProcess.SalesForceProcess()

df=obj.read_data(['of_No_Shows__c','Customer_Billing_Name__c','AccountId','Account_Type__c','Actual_ACV__c','Amount','Actual_ACV_Approved_by_Finance__c','Approval_Status__c',
'Arrears__c','Augmedix_Product__c','Average_Monthly_User_Value__c','BDR_Marketing_Prospecting__c','Billing_Point_of_Contact__c','Budget__c','Budget_Type__c',
'Clinic_Chart__c','CloseDate','IsClosed','Closed_Lost_Explanation__c','Closed_Lost_Reason__c','Competition_Name__c','Competition_Present__c','Competition_Pricing_Details__c',
'Contract_Signer__c','Contract_Terms__c','CreatedById','CreatedDate','Created_From_Lead__c','Current_Documentation_Solution__c','Current_User_Upgrade__c',
'Customer_Point_of_Contact__c','Customer_point_of_contact_Email__c','Decision_Maker_Contact_Information__c','Decision_Maker_Name__c',
'Decision_Maker_Titles__c','Demo_Scheduled__c','Description','Opportunity_EHR__c','Enterprise_Level_Deal__c','Deposit__c',
'Estimated_ACV_with_Pilot__c','Total_ACV_for_Pilot_Only__c','Revenue__c','Fiscal','FiscalQuarter','FiscalYear','ForecastCategoryName',
'General_Notes__c','HasOpportunityLineItem','HasOpenActivity','HasOverdueTask','How_Did_You_Hear_About_Augmedix__c','HubSpot_Original_Source__c',
'HubSpot_Original_source_drill_down_1__c','HubSpot_Original_source_drill_down_2__c','Ideal_Start_Date__c','If_Travel_covered_input_amount__c','Implementation_Fee__c',
'Incentives_Given__c','Incentives_Given_Details__c','Include_in_90_Day_Forecast__c','Industry_Segment__c','Invoicing_Type__c','Is_Budget_Known__c','LastActivityDate',
'LastModifiedById','LastModifiedDate','Lead_Owner__c','Lead_Owner_Comments__c','LeadSource','List_of_Products__c','List_of_Users__c','Marketing_Campaign__c','Marketing_Channel__c',
'Marketing_Content__c','Marketing_Medium__c','Marketing_Source__c','Measures_of_Success__c','Name','Names_of_Initial_providers__c','Names_of_initial_cohort_known__c',
'Near_Term_Expansion_Possible__c','New_Logo_Rubric_Review__c','NextStep','Next_Step_Due_Date__c','Nurture_Reason__c','Opportunity_Age__c','Id','Type','OwnerId','Payment_Term__c',
'Pilot_Length__c','Pilot_Pricing__c','Pilot_No_Pilot__c','Prepayment__c','Pricebook2Id','Probability','RecordTypeId','Referer__c','Referer_First_Name__c','Referer_Last_Name__c',
'SFDC_Lead_ID__c','Sales__c','SalesLoft1__Primary_Contact__c','Service_Order_Type__c','Service_Order_Valid_Through__c','Source__c','Source_2__c','Specialties__c','StageName',
'Total_Pilot_ACV__c','Total_Number_of_Doctors_In_Enterprise__c','Travel_Expense_Covered__c','Users_Remote_OUS__c','Users_On_site__c','Users_Total__c','Users_Remote_US__c',
'Forecasted_Revenue__c','Weighted_Total_Users__c','IsWon','Active_or_Target_Opportunity__c','High_Quality_Enterprise_Opp__c'],"Opportunity")
df=obj.filter_blanks(df,0,'all')
df=obj.replace_none(df)

df=df.astype(str)
sparkDF=spark.createDataFrame(df) 

sparkDF.write.\
            format("parquet").\
            mode("overwrite").\
            save("s3://axai/Analytics_Data_Lake/Salesforce_Data/Opportunities/")

job.commit()
