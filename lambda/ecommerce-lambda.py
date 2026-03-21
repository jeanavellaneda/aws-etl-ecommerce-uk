import json
import boto3
import os

print("=== LAMBDA INICIANDO ===")
print(f"STEP_ARN desde entorno: {os.environ.get('STEP_ARN', 'NO EXISTE')}")

stepfunctions = boto3.client('stepfunctions')
STEP_ARN = os.environ['STEP_ARN']

def lambda_handler(event, context):
    print("=== HANDLER EJECUTANDOSE ===")
    print(f"Tipo de event: {type(event)}")
    print(f"Event completo: {json.dumps(event, indent=2)}")
    
    try:
        # Extraer datos correctamente
        s3_bucket = event['detail']['bucket']['name']
        s3_key = event['detail']['object']['key']
        
        print(f"Bucket encontrado: {s3_bucket}")
        print(f"Key encontrado: {s3_key}")
        
        input_payload = {
            "input_key": s3_key,
            "input_bucket": s3_bucket
        }
        
        print(f"Iniciando Step Function: {STEP_ARN}")
        
        response = stepfunctions.start_execution(
            stateMachineArn=STEP_ARN,
            input=json.dumps(input_payload)
        )
        
        print(f"Step Function iniciada: {response['executionArn']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({'executionArn': response['executionArn']})
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }