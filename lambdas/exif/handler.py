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

def exif_handler(event, context):
    """
    EXIF Lambda - Process all images in the event
    """
    print("EXIF Lambda triggered")
    print(f"Event received with {len(event.get('Records', []))} SNS records")

    processed_count = 0
    failed_count = 0

    # --- 1. Hard-coded settings ---
    # To use a different bucket, set this to a string: "my-target-bucket-name"
    target_bucket = None 
    # The S3 "folder" to put the extracted .json files in.
    target_prefix = 'exif' 

    if target_prefix and not target_prefix.endswith('/'):
        target_prefix += '/'

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

                    # exif lambda code
                    # Check file extension to ensure we only process images
                    file_extension = Path(object_key).suffix.lower()
                    if file_extension not in ['.jpg', '.jpeg', '.tif', '.tiff']:
                        print(f"Skipping non-image file: {object_key}")
                        continue  # Go to the next s3_event

                    # --- 3. Download the image ---
                    image = download_from_s3(bucket_name, object_key)

                    # --- 4. Extract and clean EXIF data ---
                    raw_exif = image._getexif()
                    exif_data_clean = {}

                    if raw_exif:
                        for tag_id, value in raw_exif.items():
                            # Get human-readable tag name
                            tag_name = Image.ExifTags.TAGS.get(tag_id, tag_id)
                            
                            # Clean data to be JSON serializable
                            if isinstance(value, bytes):
                                clean_value = value.decode('utf-8', 'ignore')
                            # Convert other complex types to string
                            elif not isinstance(value, (str, int, float, bool, type(None))):
                                clean_value = str(value)
                            else:
                                clean_value = value
                            
                            exif_data_clean[tag_name] = clean_value
                    else:
                        print(f"No EXIF data found for {object_key}")

                    # --- 5. Define new key and upload JSON ---
                    p = Path(object_key)
                    # New key is 'exif/filename.json'
                    new_key = f"{target_prefix}{p.stem}.json"
                    
                    json_data = json.dumps(exif_data_clean, indent=2)

                    upload_to_s3(target_bucket, new_key, json_data, content_type='application/json')
                    

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
