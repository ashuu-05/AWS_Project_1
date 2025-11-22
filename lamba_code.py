import json
import boto3
import pandas as pd
import os

# Initialize clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')  # <--- Added Glue Client

# Define your Crawler Name here
CRAWLER_NAME = 'etl-crawler' 

def lambda_handler(event, context):
    try:
        # 1. Extract Bucket Name and File Key from the event
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        input_key = record['s3']['object']['key']
        
        print(f"Processing file: {input_key} from bucket: {bucket_name}")

        # 2. Define paths for temporary processing
        file_name = input_key.split('/')[-1]
        download_path = f'/tmp/{file_name}'
        parquet_file_name = file_name.replace('.json', '.parquet')
        upload_path = f'/tmp/{parquet_file_name}'
        
        # Define the S3 Output Key
        output_key = input_key.replace('input/', 'output/').replace('.json', '.parquet')

        # 3. Download the JSON file from S3
        s3_client.download_file(bucket_name, input_key, download_path)
        
        # 4. READ & FLATTEN DATA
        with open(download_path, 'r') as f:
            json_data = json.load(f)

        # A. Flatten structs
        df = pd.json_normalize(json_data)

        # B. Flatten Arrays
        if 'products' in df.columns:
            df = df.explode('products').reset_index(drop=True)
            
            # C. Flatten content of exploded array
            if not df.empty and isinstance(df['products'].iloc[0], dict):
                products_normalized = pd.json_normalize(df['products'])
                products_normalized = products_normalized.add_prefix('products.')
                df = df.drop('products', axis=1).join(products_normalized)

        print("----- Data Preview (Flattened) -----")
        print(df.head())

        # 5. Convert DataFrame to Parquet
        df.to_parquet(upload_path, index=False)
        
        # 6. Upload the new Parquet file back to S3
        s3_client.upload_file(upload_path, bucket_name, output_key)
        print(f"Successfully uploaded parquet file to: s3://{bucket_name}/{output_key}")

        # 7. TRIGGER GLUE CRAWLER (New Step)
        print(f"Triggering Glue Crawler: {CRAWLER_NAME}...")
        glue_client.start_crawler(Name=CRAWLER_NAME)
        print("Crawler started successfully.")

        return {
            'statusCode': 200,
            'body': json.dumps(f'File converted, uploaded to {output_key}, and crawler triggered.')
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }