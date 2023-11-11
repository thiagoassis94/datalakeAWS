import os
import json
import boto3

# SO variable
bucket_path = os.environ['bucket_path']

# Start boto3
s3_client = boto3.client('s3')

# Main script
def lambda_handler(event, context):
    s3_bucket_source = event['Records'][0]['s3']['bucket']['name'] # records é do arquivo, 0 é a posição inicial
    s3_object_key = event['Records'][0]['s3']['object']['key']
    s3_bucket_destination = bucket_path
    copy_object = {'Bucket': s3_bucket_source, 'Key': s3_object_key}
    s3_client.copy_object(CopySource=copy_object, Bucket=s3_bucket_destination, Key=s3_object_key)

    return{
        'body': json.dumps('File has been successfully copied')
    }