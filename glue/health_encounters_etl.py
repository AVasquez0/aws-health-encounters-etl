import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import concurrent.futures
import re

# Replace <your-processed-bucket> with your own S3 bucket name before running.

# Script generated for node Clear old output
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    glueContext.purge_s3_path(
        "s3://<your-processed-bucket>/encounters_partitioned/",
        {"retentionPeriod": 0}
    )
    return dfc
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1792548978923 = glueContext.create_dynamic_frame.from_catalog(database="health_pipeline_db", table_name="patients", transformation_ctx="AWSGlueDataCatalog_node1792548978923")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1792548940087 = glueContext.create_dynamic_frame.from_catalog(database="health_pipeline_db", table_name="encounters", transformation_ctx="AWSGlueDataCatalog_node1792548940087")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1792550149631 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1792548978923, mappings=[("patient_id", "string", "right_patient_id", "string"), ("birth_year", "long", "right_birth_year", "long"), ("sex", "string", "right_sex", "string"), ("state", "string", "right_state", "string"), ("insurance_type", "string", "right_insurance_type", "string")], transformation_ctx="RenamedkeysforJoin_node1792550149631")

# Script generated for node Join
Join_node1792549084352 = Join.apply(frame1=AWSGlueDataCatalog_node1792548940087, frame2=RenamedkeysforJoin_node1792550149631, keys1=["patient_id"], keys2=["right_patient_id"], transformation_ctx="Join_node1792549084352")

# Script generated for node Change Schema
ChangeSchema_node1792550528820 = ApplyMapping.apply(frame=Join_node1792549084352, mappings=[("encounter_id", "string", "encounter_id", "string"), ("patient_id", "string", "patient_id", "string"), ("encounter_date", "string", "encounter_date", "date"), ("encounter_type", "string", "encounter_type", "string"), ("department", "string", "department", "string"), ("provider_id", "string", "provider_id", "string"), ("diagnosis_code", "string", "diagnosis_code", "string"), ("diagnosis_desc", "string", "diagnosis_desc", "string"), ("length_of_stay_days", "long", "length_of_stay_days", "int"), ("billed_amount", "double", "billed_amount", "double"), ("paid_amount", "double", "paid_amount", "double"), ("readmitted_30d", "string", "readmitted_30d", "string"), ("right_birth_year", "long", "birth_year", "int"), ("right_sex", "string", "sex", "string"), ("right_state", "string", "state", "string"), ("right_insurance_type", "string", "insurance_type", "string")], transformation_ctx="ChangeSchema_node1792550528820")

# Script generated for node Add year month
SqlQuery0 = '''
SELECT *,
        year(encounter_date) AS year,
        month(encounter_date) AS month
FROM myDataSource
'''
Addyearmonth_node1792607812481 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1792550528820}, transformation_ctx = "Addyearmonth_node1792607812481")

# Script generated for node DQ checks
DQchecks_node1792611886814_ruleset = """
    Rules = [ IsComplete "encounter_id", IsUnique "encounter_id", IsComplete "patient_id", IsComplete "encounter_date", ColumnValues "billed_amount" > 0, ColumnValues "paid_amount" >= 0, ColumnValues "length_of_stay_days" >= 0, ColumnValues "readmitted_30d" in ["Y", "N"] ]
"""

DQchecks_node1792611886814 = EvaluateDataQuality().process_rows(frame=Addyearmonth_node1792607812481, ruleset=DQchecks_node1792611886814_ruleset, publishing_options={"dataQualityEvaluationContext": "DQchecks_node1792611886814", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1792611936599 = SelectFromCollection.apply(dfc=DQchecks_node1792611886814, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1792611936599")

# Script generated for node ruleOutcomes
ruleOutcomes_node1792611944693 = SelectFromCollection.apply(dfc=DQchecks_node1792611886814, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1792611944693")

# Script generated for node Conditional Router
ConditionalRouter_node1792612190919 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1792611936599,
  group_filters = [GroupFilter(name = "output_group_1", filters = lambda row: (bool(re.match("Passed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Passed", row["DataQualityEvaluationResult"])))))])

# Script generated for node output_group_1
output_group_1_node1792612191755 = SelectFromCollection.apply(dfc=ConditionalRouter_node1792612190919, key="output_group_1", transformation_ctx="output_group_1_node1792612191755")

# Script generated for node default_group
default_group_node1792612191652 = SelectFromCollection.apply(dfc=ConditionalRouter_node1792612190919, key="default_group", transformation_ctx="default_group_node1792612191652")

# Script generated for node Drop DQ columns
DropDQcolumns_node1792612303127 = DropFields.apply(frame=output_group_1_node1792612191755, paths=["DataQualityRulesPass", "DataQualityRulesFail", "DataQualityEvaluationResult", "DataQualityRulesSkip"], transformation_ctx="DropDQcolumns_node1792612303127")

# Script generated for node Clear old output
Clearoldoutput_node1792616464403 = MyTransform(glueContext, DynamicFrameCollection({"DropDQcolumns_node1792612303127": DropDQcolumns_node1792612303127}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1792616603723 = SelectFromCollection.apply(dfc=Clearoldoutput_node1792616464403, key=list(Clearoldoutput_node1792616464403.keys())[0], transformation_ctx="SelectFromCollection_node1792616603723")

# Script generated for node Quarantine S3
EvaluateDataQuality().process_rows(frame=default_group_node1792612191652, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792611851783", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
QuarantineS3_node1792612692654 = glueContext.write_dynamic_frame.from_options(frame=default_group_node1792612191652, connection_type="s3", format="glueparquet", connection_options={"path": "s3://<your-processed-bucket>/quarantine/encounters/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="QuarantineS3_node1792612692654")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1792616603723, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792548643802", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1792553627606 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1792616603723, connection_type="s3", format="glueparquet", connection_options={"path": "s3://<your-processed-bucket>/encounters_partitioned/", "partitionKeys": ["year", "month"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1792553627606")

job.commit()

