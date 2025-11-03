import boto3
import json
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    try:
        # Entrada (json)
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event.get('body', {})
        nombre_bucket = body.get('bucket')
        nombre_directorio = body.get('directorio')
        
        if not nombre_bucket or not nombre_directorio:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Faltan parámetros requeridos',
                    'message': 'Debe proporcionar tanto el nombre del bucket como el nombre del directorio'
                })
            }
        
        # Asegurar que el nombre del directorio termine con '/'
        if not nombre_directorio.endswith('/'):
            nombre_directorio += '/'
        
        # Proceso
        s3 = boto3.client('s3')
        
        # Verificar que el bucket existe
        try:
            s3.head_bucket(Bucket=nombre_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'error': 'El bucket no existe',
                        'bucket': nombre_bucket
                    })
                }
            raise e
        
        # Crear el directorio (objeto vacío con key que termina en '/')
        s3.put_object(
            Bucket=nombre_bucket,
            Key=nombre_directorio,
            Body=b''
        )
        
        # Salida
        return {
            'statusCode': 201,
            'body': json.dumps({
                'message': 'Directorio creado exitosamente',
                'bucket': nombre_bucket,
                'directorio': nombre_directorio
            })
        }
        
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Error al crear el directorio',
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
