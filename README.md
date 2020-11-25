# vis-1
load files in database using yaml and print status message on logs



import sys
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "ibcontractjsondatafroms3", table_name = "ibcontract_json", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "ibcontractjsondatafroms3", table_name = "ibcontract_json", transformation_ctx = "datasource0")

try:
    
    glue_df = glueContext.create_dynamic_frame.from_options(
    's3',
    {"paths": ['s3://cp-prod-dwh-assets/sftp_json/mndb/crmP.json']},
    "json")
    
    print( "input row count is: ", glue_df.count())
    rowcount_input=glue_df.count()


## @type: ApplyMapping
## @args: [mapping = [("_id", "string", "_id", "string"), ("partyid", "string", "partyid", "string"), ("timecreated.$date", "string", "timecreated", "string"), ("timelastchanged.$date", "string", "timelastchanged", "string"), ("users", "array", "users", "string"), ("status", "string", "status", "string"), ("blockreason", "string", "blockreason", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
    applymapping1= ApplyMapping.apply(frame = glue_df, mappings = [
("_id", "string", "id", "string"),
("basicinfo.fullName", "string", "`basicinfo.fullName`", "string"),
("basicinfo.relatedToInstitution", "boolean", "`basicinfo.relatedToInstitution`", "boolean"), 
("basicinfo.underRegistration", "boolean", "`basicinfo.underRegistration`", "boolean"),
("basicinfo.registrationCountry", "string", "`basicinfo.registrationCountry`", "string"), 
("basicinfo.residenceCountries", "array", "`basicinfo.residenceCountries`", "string"), 
("questionnaire.turnover.monthlyTurnoverInCash.value.$numberDecimal", "string", "`q.turnover.monthlyTurnoverInCash.value`", "string"), 
("questionnaire.riskProfile.amlRiskProfile", "string", "`q.riskProfile.amlRiskProfile`", "string"), 
("questionnaire.riskProfile.fatcaStatus", "string", "`q.riskProfile.fatcaStatus`", "string"), 
("questionnaire.accountsInOtherInstitutions", "array", "`q.accountsInOtherInstitutions`", "string"), 
("questionnaire.operatingRegions", "array", "`q.operatingRegions`", "string"), 
("questionnaire.mainBusinessActivities", "array", "`q.mainBusinessActivities`", "string"), 
("questionnaire.activityCountries", "array", "`q.activityCountries`", "string"), 
("questionnaire.accountOpeningReasons", "array", "`q.accountOpeningReasons`", "string"), 
("questionnaire.mainSourcesOfIncome", "array", "`q.mainSourcesOfIncome`", "string"), 
("questionnaire.preferredOperations", "array", "`q.preferredOperations`", "string"), 
("questionnaire.relationToCountry", "array", "`q.relationToCountry`", "string"), 
("questionnaire.activities", "array", "`q.activities`", "string"), 
("questionnaire.otherBusinessActivity", "string", "`q.otherBusinessActivity`", "string"), 
("questionnaire.activityInPreferentialTaxZones", "boolean", "`q.activityInPreferentialTaxZones`", "boolean"), 
("questionnaire.otherSourcesOfIncome", "string", "`q.otherSourcesOfIncome`", "string"), 
("questionnaire.purposeOfBusinessRelationship", "string", "`q.purposeOfBusinessRelationship`", "string"), 
("questionnaire.otherActivities", "string", "`q.otherActivities`", "string"),
("partytype", "string", "partytype", "string"),
("contact.language", "string", "`contact.language`", "string"), 
("contact.mainEmail", "string", "`contact.mainEmail`", "string"), 
("contact.emails", "array", "`contact.emails`", "string"), ("contact.mainPhoneNumber", "string", "`contact.mainPhoneNumber`", "string"), 
("contact.mainPhoneType", "string", "`contact.mainPhoneType`", "string"), ("contact.phones", "array", "`contact.phones`", "string"),
("relationships", "array", "relationships", "string"), 
("authorizedpersons", "array", "authorizedpersons", "string"), 
("createdon.$date", "string", "`createdon.$date`", "timestamp"), 
("iddocuments", "array", "iddocuments", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
    resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
    dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @return: datasink4
## @inputs: [frame = dropnullfields3]

    db_properties={"user":"aaaaaaaaa", "password": "11111222222"}
    dataDf    =   dropnullfields3.toDF()
    dataDf.write.option("truncate", "true").jdbc(url='jdbc:-------------------', table='crmpary', mode="overwrite", properties=db_properties)



    print("output count",dataDf.count())
    rowcount_output=dataDf.count()

#datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "CP-RDS-AuroraDBMySQL", connection_options = {"dbtable": "ibcontract", "database": "cpdb"}, transformation_ctx = "datasink1")



    if rowcount_input == rowcount_output :
        print("counts are same")
    else:
         message = {"input count": rowcount_input, "output count": rowcount_output}
         client = boto3.client('sns', region_name='eu-west-1')
         response = client.publish(
         TargetArn="arn:aws:sns:eu-west-1:00999:gluetest",
         Message=json.dumps({'default': json.dumps(message)}),
         Subject=' crnpary -Job Failed',
         MessageStructure='json'
          )
          
except Exception as e:
   error_message = {}
   error_message['error_messag'] = f'{e}'
   client = boto3.client('sns', region_name='eu-west-1')
   response = client.publish(
   TargetArn="arn:aws:sns:eu-west-1:070723:gluetest",
   Message=json.dumps({'default': json.dumps( error_message)}),
   Subject=' crmpary -Job Failed',
   MessageStructure='json'
          )
