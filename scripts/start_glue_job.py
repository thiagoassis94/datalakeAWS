import json
import uuid
import os
import boto3
import dateutil.parser
from urllib.parse import unquote_plus

stepFunction_cli = boto3.client('stepfunctions')
STATE_MACHINE_ARN = os.getenv("state_machine_arn")

def lambda_handler(event, context):
    print(event)

    for record in event['Records']:
        s3_bucket_name = record['s3']['bucket']['name']
        s3_object_key = unquote_plus(record['s3']['object']['key'])
        s3_object_extension = record['s3']['object']['key'].split('.')[-1]
        s3_file_name = record['s3']['object']['key'].split('/')[-1]
        guid = str(uuid.uuid4())
        upload_date = dateutil.parser.parse(record['eventTime'])

        json_state_machine = {
            "originalFile":{
                "s3BucketName": s3_bucket_name,
                "s3ObjectKey": s3_object_key,
                "s3ObjectExtension": s3_object_extension,
                "s3FileName": s3_file_name,
                "uploadDate": upload_date.strftime("%Y-%m-%d %H:%M") # %Y retorna 4 digitos do ano, %m retorna os dois digitos do mês
            },
            "guid": guid
        }

        stepFunction_cli.start_execution(
            stateMachineArn= STATE_MACHINE_ARN,
            input=json.dumps(json_state_machine),
            name=guid
        )
    
    return True