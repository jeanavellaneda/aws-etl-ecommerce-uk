import json
import boto3
import os

stepfunctions = boto3.client('stepfunctions')
STEP_ARN = os.environ['STEP_ARN']

def lambda_handler(event, context):
    try:
        logger.info("=== Lambda invocada ===")
        print(f"Event recibido: {json.dumps(event, indent=2)}")
        
        s3_key = event['detail']['object']['key']
        s3_bucket = event['detail']['bucket']['name']

        input_payload = {
            "input_key": s3_key,
            "input_bucket": s3_bucket
        }
        
        print(f"Iniciando Step Function: {STEP_ARN}")
        print(f"Input: {json.dumps(input_payload)}")

        response = stepfunctions.start_execution(
            stateMachineArn=STEP_ARN,
            input=json.dumps(input_payload)
        )

        logger.info(f"Step Function ejecutada: {response['executionArn']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': "Glue Job ejecutado correctamente",
                'JobRunId': response['JobRunId']
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error al ejecutar Step Function: {str(e)}')
        }
