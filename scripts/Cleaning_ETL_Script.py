from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark context and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the database name
database_name = ""

# Read JSON data from S3 into a DataFrame
json_df = spark.read.json("s3://bucket/crime_data.json")

# Transform the DataFrame to match the desired schema, remove null values, and deduplicate
records_df = json_df.select(json_df.record_id.cast("int").alias("record_id"),
                             json_df.dr_no.cast("int").alias("dr_no")) \
                    .dropna(subset=["record_id"]) \
                    .dropDuplicates(['record_id'])

victims_df = json_df.select(json_df.record_id.cast("int").alias("record_id"),
                             json_df.age.cast("int").alias("age"),
                             json_df.sex.cast("string").alias("sex"),
                             json_df.descent.cast("string").alias("descent")) \
                    .dropna(subset=["record_id"]) \
                    .dropDuplicates(['record_id'])

status_df = json_df.select(json_df.status.cast("string").alias("status"),
                            json_df.status_desc.cast("string").alias("status_desc")).dropDuplicates(['status'])

premises_df = json_df.select(json_df.premis_cd.cast("int").alias("premis_cd"),
                              json_df.premis_desc.cast("string").alias("premis_desc")) \
                     .dropna(subset=["premis_cd"]) \
                     .dropDuplicates(['premis_cd'])

areas_df = json_df.select(json_df.area.cast("int").alias("area"),
                           json_df.area_name.cast("string").alias("area_name")) \
                  .dropna(subset=["area"]) \
                  .dropDuplicates(['area'])

weapons_df = json_df.select(json_df.weapon_used_cd.cast("int").alias("weapon_used_cd"),
                             json_df.weapon_desc.cast("string").alias("weapon_desc")) \
                    .dropna(subset=["weapon_used_cd"]) \
                    .dropDuplicates(['weapon_used_cd'])

locations_df = json_df.select(json_df.record_id.cast("int").alias("record_id"),
                               json_df.location.cast("string").alias("location"),
                               json_df.cross_street.cast("string").alias("cross_street"),
                               json_df.rpt_dist_no.cast("int").alias("rpt_dist_no"),
                               json_df.premis_cd.cast("int").alias("premis_cd"),
                               json_df.area.cast("int").alias("area"),
                               json_df.lat.cast("float").alias("lat"),
                               json_df.lon.cast("float").alias("lon")) \
                      .dropna(subset=["record_id"]) \
                      .dropDuplicates(['record_id'])

crime_codes_df = json_df.select(json_df.crm_cd.cast("int").alias("crm_cd"),
                                 json_df.crm_cd_desc.cast("string").alias("crm_cd_desc")) \
                        .dropna(subset=["crm_cd"]) \
                        .dropDuplicates(['crm_cd'])

crimes_df = json_df.select(json_df.record_id.cast("int").alias("record_id"),
                            json_df.crm_cd.cast("int").alias("crm_cd"),
                            json_df.other_crm_cds.cast("string").alias("other_crm_cds"),
                            json_df.mocodes.cast("string").alias("mocodes")) \
                   .dropna(subset=["record_id"]) \
                   .dropDuplicates(['record_id'])

crime_incidents_df = json_df.select(json_df.record_id.cast("int").alias("record_id"),
                                     json_df.date_rptd.cast("date").alias("date_rptd"),
                                     json_df.date_occ.cast("date").alias("date_occ"),
                                     json_df.time_occ.cast("int").alias("time_occ"),
                                     json_df.crm_cd.cast("int").alias("crm_cd"),
                                     json_df.weapon_used_cd.cast("int").alias("weapon_used_cd"),
                                     json_df.status.cast("string").alias("status"),
                                     json_df.part_1_2.cast("int").alias("part_1_2")) \
                            .dropna(subset=["record_id", "crm_cd", "weapon_used_cd"]) \
                            .dropDuplicates(['record_id'])

# Write DataFrames to the Glue Data Catalog within the specified database
records_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Records").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Records")
victims_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Victims").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Victims")
status_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Status").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Status")
premises_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Premises").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Premises")
areas_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Areas").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Areas")
weapons_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Weapons").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Weapons")
locations_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Locations").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Locations")
crime_codes_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Crime_Codes").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Crime_Codes")
crimes_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Crimes").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Crimes")
crime_incidents_df.write.format("parquet").mode("overwrite").option("path", "s3://bucket/Crime_Incidents").option("createTableColumnTypes", "snappy").saveAsTable(database_name + ".Crime_Incidents")
