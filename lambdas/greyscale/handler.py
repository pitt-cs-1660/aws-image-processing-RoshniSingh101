import json
from PIL import Image
import io
import boto3
from pathlib import Path

def download_from_s3(bucket, key):
    s3 = boto3.client('s3')
    buffer = io.BytesIO()
    s3.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return Image.open(buffer)

def upload_to_s3(bucket, key, data, content_type='image/jpeg'):
    s3 = boto3.client('s3')
    if isinstance(data, Image.Image):
        buffer = io.BytesIO()
        data.save(buffer, format='JPEG')
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, key)
    else:
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def greyscale_handler(event, context):
    """
    Greyscale Lambda - Process all images in the event
    """
    print("Greyscale Lambda triggered")
    print(f"Event received with {len(event.get('Records', []))} SNS records")

    processed_count = 0
    failed_count = 0

    # iterate over all SNS records
    for sns_record in event.get('Records', []):
        try:
            # extract and parse SNS message
            sns_message = json.loads(sns_record['Sns']['Message'])

            # iterate over all S3 records in the SNS message
            for s3_event in sns_message.get('Records', []):
                try:
                    s3_record = s3_event['s3']
                    bucket_name = s3_record['bucket']['name']
                    object_key = s3_record['object']['key']

                    print(f"Processing: s3://{bucket_name}/{object_key}")

                    # add greyscale lambda code here
                    # Use Pathlib to easily manage paths
                    original_path = Path(object_key)

                    # 1. Prevent infinite loops by checking if already processed
                    # If the first part of the path is 'greyscale', skip it.
                    if original_path.parts and original_path.parts[0] == 'greyscale':
                        print(f"Skipping already processed file: {object_key}")
                        continue # Move to the next s3_event

                    # 2. Download Image
                    print(f"Downloading {object_key}...")
                    image = download_from_s3(bucket_name, object_key)

                    # 3. Convert to Greyscale
                    print(f"Converting {object_key} to greyscale...")
                    greyscale_image = image.convert('L') # 'L' mode is for 8-bit greyscale

                    # 4. Define Destination
                    # We'll save to the same bucket, but in a 'greyscale/' prefix
                    destination_key = f"greyscale/{original_path.name}"
                    
                    # 5. Upload Processed Image
                    print(f"Uploading to s3://{bucket_name}/{destination_key}")
                    upload_to_s3(bucket_name, destination_key, greyscale_image, content_type='image/jpeg')

                    print(f"Successfully processed {object_key}")

                    processed_count += 1

                except Exception as e:
                    failed_count += 1
                    error_msg = f"Failed to process {object_key}: {str(e)}"
                    print(error_msg)

        except Exception as e:
            print(f"Failed to process SNS record: {str(e)}")
            failed_count += 1

    summary = {
        'statusCode': 200 if failed_count == 0 else 207,  # @note: 207 = multi-status
        'processed': processed_count,
        'failed': failed_count,
    }

    print(f"Processing complete: {processed_count} succeeded, {failed_count} failed")
    return summary
