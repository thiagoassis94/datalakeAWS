import sys
import json
import boto3
import pandas as pd
from math import ceil
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET',
                                     'S3_SOURCE', 'S3_JSON',
                                     'FILE_NAME', 'LOG_PATH',
                                     'JSON_FILE', 'PROCESS_TYPE'])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

s3 = boto3.resource('s3')

job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
bucket_source = args['S3_SOURCE']
bucket_target = args['S3_TARGET']
bucket_json = args['S3_JSON']
json_file = args['JSON_FILE']
file_name = args['FILE_NAME']
log_path = args['LOG_PATH']
process_type = args['PROCESS_TYPE']

#############################
# Functions definitions
def save_log(process_type, job_run_id, status, n_rows, start_time, end_time, elapsed_time):
    log_data = {
        'process_data': process_type,
        'job_id': job_run_id,
        'status': status,
        'total_registers': n_rows,
        'start_time': start_time,
        'end_time': end_time,
        'elapsed_time': elapsed_time
    }

    try:
        df1 = pd.read_csv('{}{}.csv'.format(log_path, job_name))
        df2 = pd.DataFrame([log_data])
        df3 = df1.append(df2)
        df3.to_csv('{}{}.csv'.format(log_path, job_name), index=False)
    except:
        df = pd.DataFrame([log_data])
        df.to_csv('{}{}.csv'.format(log_path, job_name), index=False)

def lower_column_names(df):
    return df.toDF(*[c.lower() for c in df.columns])

def standarlize_column_names(df):
    return df.toDF(*[c.strip("\t") for c in df.columns])

def get_struct_type(column_name, schema):
    for field in schema["columns"]:
        if column_name.lower() == field["name"].lower():
            return field

def load_json(bucket_name, bucket_key):
    content_object = s3.Object(bucket_name, bucket_key)
    file_content = content_object.get()['Body'].read().decode("utf-8")
    json_file = json.loads(file_content)
    return json_file

def load_csv(path, col, spark):
    df = spark.read.option("delimiter", ",") \
                   .option("header", True) \
                   .csv(path)
    df = df.toDF(*col)
    return df
############################################
# Main Script

# Principal Parameter
MAX_REGISTERS_REPARTITION = 250000 #(variável para configurar algo, sempre em UPPER CASE) o valor de 250000 foi escolhido por conta da experiência do Edinor

# Start date from script
start_time = datetime.now()
start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S") #formatação da data e hora em string
print(f'Script starts: {start_time_str}') #outras formas de fazer essa formula "print("script starts: {}".format(start_time_str))" e "print("scripts starts: " + start_time_str)"

# Load json
json_schema = load_json(bucket_json, json_file)
columns = [x["name"] for x in json_schema["columns"]]

# Load csv
df = load_csv('{}{}'.format(bucket_source, file_name), columns, spark)

# Make a standard column name
output = standarlize_column_names(df)

# Make lower column name
df_final = lower_column_names(output)

# Changing the final data frame schema
for column in df_final.columns:
    schema_field = get_struct_type(column, json_schema)
    try:
        df_final = df_final.withColumn(column, col(column).cast(schema_field["type"]))
        df_final = df_final.withColumnRenamed(column, schema_field["name"])
    except:
        df_final = df_final.withColumn(column, col(column).cast("string"))
        df_final = df_final.withColumnRenamed(column, schema_field["name"])

# Count register
total_registers = df_final.count()
print(f'Total rows: {total_registers}')

# Repartition of data
num_repartitions = ceil(total_registers / MAX_REGISTERS_REPARTITION)
df_final = df_final.repartition(num_repartitions)

# Write Data Frame in Bucket S3 / snappy mais comum de ser usado pela experiência / overwrite para atualizar tudo (carga full), se fosse incremental (update), seria append
df_final.write.format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("overwrite") \
        .save(bucket_target)

# Script end time
end_time = datetime.now()
end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
print(f"Script's end: {end_time_str}")

# Write log with results
status = "SUCCESS" 
n_row = total_registers
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, n_row, start_time, end_time, elapsed_time)

print(f"Execution time: {elapsed_time}")

job.commit()