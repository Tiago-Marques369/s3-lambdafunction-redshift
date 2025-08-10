import boto3  # AWS SDK, not used directly here but useful for other operations
import psycopg2  # Driver to connect to Redshift/PostgreSQL
import logging  # To log Lambda function info and errors
import os  # To access environment variables

# Configure logger to display info and error messages
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Get the bucket and file key that triggered the Lambda via S3 event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        
        # Establish connection to Redshift using environment variables for credentials
        conn = psycopg2.connect(
            dbname=os.getenv('REDSHIFT_DB'),
            user=os.getenv('REDSHIFT_USER'),
            password=os.getenv('REDSHIFT_PASSWORD'),
            host=os.getenv('REDSHIFT_HOST'),
            port=os.getenv('REDSHIFT_PORT', '5439')  # Default Redshift port
        )
        
        # Build the SQL COPY command to load the CSV from S3 into the 'vendas' table
        copy_command = f"""
        COPY vendas FROM 's3://{s3_bucket}/{s3_key}'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789:role/IAMRoleRedshift'
        CSV
        IGNOREHEADER 1
        DELIMITER ','
        """
        
        # Execute the command and commit the transaction
        with conn.cursor() as cursor:
            cursor.execute(copy_command)
            conn.commit()
        
        # Return success if everything works
        return {'statusCode': 200, 'body': 'Success!'}
    
    except Exception as e:
        # Log the error and return a 500 status with the error message
        logger.error(f"ERROR: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}
    
    finally:
        # Ensure the connection is closed even if there is an error
        if 'conn' in locals():
            conn.close()
