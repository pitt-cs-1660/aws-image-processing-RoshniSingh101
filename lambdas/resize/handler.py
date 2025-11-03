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

def resize_handler(event, context):
    """
    Resize Lambda - Process all images in the event
    """
    print("Resize Lambda triggered")
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

                    # resize lambda code here
                    # 1. Define target size and location
                    target_size = (200, 200)  # Desired thumbnail size (width, height)
                    source_path = Path(object_key)
                    
                    # Store thumbnails in a 'thumbnails/' prefix with the same filename
                    # e.g., 'uploads/image.jpg' -> 'thumbnails/image.jpg'
                    target_key = f"thumbnails/{source_path.name}"
                    target_bucket = bucket_name

                    # 2. CRITICAL: Prevent infinite loops
                    # If the event is for a file *already* in the thumbnails dir, skip it.
                    if object_key.startswith('thumbnails/'):
                        print(f"Skipping already processed file: {object_key}")
                        continue  # Move to the next S3 record

                    # 3. Download the original image
                    print(f"Downloading s3://{bucket_name}/{object_key}...")
                    image = download_from_s3(bucket_name, object_key)
                    print(f"Original size: {image.size}")

                    # 4. Resize the image
                    # We use .thumbnail() to maintain aspect ratio.
                    # It resizes the image *in-place* to fit *within* the target_size.
                    image.thumbnail(target_size, Image.Resampling.LANCZOS)
                    print(f"New size: {image.size}")

                    # 5. Upload the new thumbnail image
                    print(f"Uploading to s3://{target_bucket}/{target_key}...")
                    upload_to_s3(target_bucket, target_key, image)
                    print("Upload complete.")

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
