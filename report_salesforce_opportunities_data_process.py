import sys
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



opportunity_df = spark.read.\
           format("parquet").\
           load("s3://axai//Analytics_Data_Lake/Salesforce_Data/Opportunities"+"/*")
           
opportunity_df = opportunity_df.replace('None', None)
opportunity_df.createOrReplaceTempView("opportunity_view")

account_df = spark.read.\
           format("parquet").\
           load("s3://axai//Analytics_Data_Lake/Salesforce_Data/Accounts"+"/*").select("Id","Name")
           
account_df = account_df.replace('None', None)
account_df.createOrReplaceTempView("account_opportunity_view")

user_df = spark.read.\
           format("parquet").\
           load("s3://axai//Analytics_Data_Lake/Salesforce_Data/Users"+"/*").select("Id","Name")
           
user_df = user_df.replace('None', None)
user_df.createOrReplaceTempView("user_opportunity_view")

recordtype_df = spark.read.\
           format("parquet").\
           load("s3://axai//Analytics_Data_Lake/Salesforce_Data/RecordType"+"/*").select('Id','Name','DeveloperName')
           
recordtype_df = recordtype_df.replace('None', None)
recordtype_df.createOrReplaceTempView("recordtype_view")

contact_df = spark.read.\
           format("parquet").\
           load("s3://axai//Analytics_Data_Lake/Salesforce_Data/Contacts"+"/*").select('Id','FirstName','LastName','Name')
           
contact_df = contact_df.replace('None', None)
contact_df.createOrReplaceTempView("contact_opportunity_view")

report_query="select op.Id as opportunity_id,op.Name as opportunity_name,u.Name as opportunity_owner,rv.Name as opportunity_record_type,DeveloperName as opportunity_record_type_name,int(Opportunity_Age__c) as opportunity_age,op.Type as opportunity_type,av.Name as account_name,op.Account_Type__c as account_type,Active_or_Target_Opportunity__c as active_or_target_opportunity,Customer_Billing_Name__c as account_billing_name,float(Actual_ACV__c) as actual_acv,float(Amount) as actual_ac_total_value_of_products,boolean(Actual_ACV_Approved_by_Finance__c) as actual_acv_approved_by_finance,Approval_Status__c as approval_status,Augmedix_Product__c as augmedix_product,BDR_Marketing_Prospecting__c as bdr_marketing_prospecting,cv.Name as billing_point_of_contact,float(Budget__c) as budget,Budget_Type__c as budget_type,to_date(CloseDate,'yyyy-MM-dd') as close_date,Closed_Lost_Explanation__c as closed_lost_explanation,Closed_Lost_Reason__c as closed_lost_reason,Competition_Name__c as competition_name,boolean(Competition_Present__c) as competition_present,Competition_Pricing_Details__c as competition_pricing_details,cv1.Name as contract_signer,Contract_Terms__c as contract_terms,u1.Name as created_by,boolean(Created_From_Lead__c) as created_from_lead,Current_Documentation_Solution__c as current_documentation_solution,Current_User_Upgrade__c as current_user_upgrade,cv2.Name as customer_point_of_contact,Customer_point_of_contact_Email__c as customer_point_of_contact_email ,Decision_Maker_Contact_Information__c as decision_maker_contact_information,Decision_Maker_Name__c as decision_maker_name,Decision_Maker_Titles__c as decision_maker_titles, Opportunity_EHR__c as ehr_name,Enterprise_Level_Deal__c as enterprise_level_deal, float(Estimated_ACV_with_Pilot__c) as estimated_acv_with_pilot, float(Total_ACV_for_Pilot_Only__c) as estimated_acv_with_pilot_only,float(Revenue__c) as estimated_annual_contract_value_acv, General_Notes__c as general_notes,How_Did_You_Hear_About_Augmedix__c as how_did_you_hear_about_augmedix,HubSpot_Original_Source__c as hubspot_original_source,HubSpot_Original_source_drill_down_1__c as hubspot_original_source_drill_down_1,HubSpot_Original_source_drill_down_2__c as hubspot_original_source_drill_down_2,to_date(Ideal_Start_Date__c,'yyyy-MM-dd') as ideal_start_date,float(Implementation_Fee__c) as implementation_fee ,Incentives_Given__c as incentives_given,Incentives_Given_Details__c as incentives_given_details ,Industry_Segment__c as industry_segment,u2.Name as last_modified_by,Lead_Owner__c as lead_owner,Lead_Owner_Comments__c as lead_owner_comments,List_of_Products__c as list_of_products,List_of_Users__c as list_of_users, Marketing_Campaign__c as marketing_campaign,Marketing_Channel__c as marketing_channel ,Marketing_Content__c as marketing_content,Marketing_Medium__c as marketing_medium,Marketing_Source__c as marketing_source, Measures_of_Success__c as measures_of_success,Names_of_Initial_providers__c as names_of_providers, Names_of_initial_cohort_known__c as names_of_cohort_known,Near_Term_Expansion_Possible__c as near_term_expansion_possible,New_Logo_Rubric_Review__c as new_enterprise_rubric_review, NextStep as next_step, to_date(Next_Step_Due_Date__c,'yyyy-MM-dd') as next_step_due_date,Nurture_Reason__c as nurture_reason,Pilot_No_Pilot__c as pilot_no_pilot, float(Probability) as probability,Referer__c as referer,Referer_First_Name__c as referrer_first_name,Referer_Last_Name__c as referrer_last_name,Sales__c as  sales_tools_used,cv3.Name as salesloft_primary_contact,Service_Order_Type__c as service_order_type,to_date(Service_Order_Valid_Through__c,'yyyy-MM-dd') as service_order_valid_through, SFDC_Lead_ID__c as sfdc_lead_id, Source__c as source,Source_2__c as source_2, int(Total_Number_of_Doctors_In_Enterprise__c) as total_no_of_doctors_in_enterprise,float(Forecasted_Revenue__c) as weighted_acv, int(Weighted_Total_Users__c) as weighted_total_users, int(Users_Total__c) as total_users, float(Pilot_Pricing__c) as pilot_pricing,int(Pilot_Length__c) as pilot_length_no_of_day, float(Deposit__c) as estimated_deposit_value,boolean(Travel_Expense_Covered__c) as travel_expense_covered,float(If_Travel_covered_input_amount__c) as if_travel_covered_estimated_amount,float(Arrears__c) as arrears,float(Prepayment__c) as prepayment,float(Average_Monthly_User_Value__c) as average_monthly_user_value,int(of_No_Shows__c) as no_of_noshows,int(Users_Remote_OUS__c) as users_ous,int(Users_On_site__c) as users_onsite,int(Users_Remote_US__c) as users_us,Fiscal as fiscalperiod,int(FiscalQuarter) as fiscalquarter,int(FiscalYear) as fiscalyear,StageName as stage,op.High_Quality_Enterprise_Opp__c as high_quality_enterprise_opp,to_date(substring(CreatedDate,0,10),'yyyy-MM-dd') as createddate,to_timestamp(substring(replace(LastModifiedDate,'T',''),0,18),'yyyy-MM-ddHH:mm:ss') as last_modified_date from opportunity_view op left join user_opportunity_view u on op.OwnerId=u.Id left join recordtype_view rv on op.RecordTypeId=rv.Id left join account_opportunity_view av on op.AccountId=av.Id left join contact_opportunity_view cv on cv.Id=op.Billing_Point_of_Contact__c left join contact_opportunity_view cv1 on cv1.Id=op.Contract_Signer__c left join  user_opportunity_view u1 on op.CreatedById=u1.Id left join contact_opportunity_view cv2 on cv2.Id=op.Customer_Point_of_Contact__c left join user_opportunity_view u2 on u2.Id=op.LastModifiedById left join contact_opportunity_view cv3 on cv3.Id=op.SalesLoft1__Primary_Contact__c where to_date(substring(LastModifiedDate,0,10),'yyyy-MM-dd')>=date_sub(current_date(),7)"

df_f = spark.sql(report_query)


if df_f.count()>0:
    df_f.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:sqlserver://axaidbinstance.ch2izlk4w3mm.us-west-1.rds.amazonaws.com:49670;databaseName=aug_bi_dw") \
        .option("dbtable", "view_salesforce_opportunities_data") \
        .option("user", "etl_user") \
        .option("password", "a&i@#pro$*2000@") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
 
job.commit()
