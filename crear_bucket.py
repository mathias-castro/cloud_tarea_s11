import boto3
import json
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    try:
        # Entrada (json)
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event.get('body', {})
        nombre_bucket = body.get('bucket')
        
        if not nombre_bucket:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'El nombre del bucket es requerido',
                    'message': 'Debe proporcionar un nombre de bucket válido'
                })
            }
        
        # Proceso
        s3 = boto3.client('s3')
        
        # Verificar si el bucket ya existe
        try:
            s3.head_bucket(Bucket=nombre_bucket)
            return {
                'statusCode': 409,
                'body': json.dumps({
                    'error': 'El bucket ya existe',
                    'bucket': nombre_bucket
                })
            }
        except ClientError as e:
            # El bucket no existe, podemos crearlo
            if e.response['Error']['Code'] != '404':
                raise e
        
        # Crear el bucket
        s3.create_bucket(Bucket=nombre_bucket)
        
        # Salida
        return {
            'statusCode': 201,
            'body': json.dumps({
                'message': 'Bucket creado exitosamente',
                'bucket': nombre_bucket
            })
        }
        
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Error al crear el bucket',
                'message': str(e)
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Error interno del servidor',
                'message': str(e)
            })
        }
