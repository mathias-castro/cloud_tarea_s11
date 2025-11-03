import boto3
import json
import base64
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    try:
        # Entrada (json)
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event.get('body', {})
        nombre_bucket = body.get('bucket')
        nombre_directorio = body.get('directorio', '')
        nombre_archivo = body.get('archivo')
        contenido_base64 = body.get('contenido_base64')
        
        if not nombre_bucket or not nombre_archivo or not contenido_base64:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Faltan parámetros requeridos',
                    'message': 'Debe proporcionar bucket, archivo y contenido_base64'
                })
            }
        
        # Construir la ruta completa del archivo
        if nombre_directorio:
            if not nombre_directorio.endswith('/'):
                nombre_directorio += '/'
            ruta_archivo = nombre_directorio + nombre_archivo
        else:
            ruta_archivo = nombre_archivo
        
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
        
        try:
            # Decodificar el contenido base64 y subir el archivo
            contenido_archivo = base64.b64decode(contenido_base64)
            
            s3.put_object(
                Bucket=nombre_bucket,
                Key=ruta_archivo,
                Body=contenido_archivo
            )
            
            # Salida
            return {
                'statusCode': 201,
                'body': json.dumps({
                    'message': 'Archivo subido exitosamente',
                    'bucket': nombre_bucket,
                    'ruta_archivo': ruta_archivo,
                    'tamaño_bytes': len(contenido_archivo)
                })
            }
            
        except Exception as decode_error:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Error al decodificar el archivo base64',
                    'message': str(decode_error)
                })
            }
        
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Error al subir el archivo',
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

def upload_base_64_to_s3(s3_bucket_name, s3_file_name, base_64_str):
    """
    Función auxiliar para subir archivos base64 a S3
    """
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket_name, s3_file_name).put(Body=base64.b64decode(base_64_str))
    return (s3_bucket_name, s3_file_name)
