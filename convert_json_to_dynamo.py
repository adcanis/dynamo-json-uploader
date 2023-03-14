import argparse
import boto3
import json

parser = argparse.ArgumentParser(description='Upload a JSON file to a DynamoDB table.')
parser.add_argument('table_name', type=str, help='Name of the DynamoDB table')
parser.add_argument('json_file_path', type=str, help='Path to the JSON file to upload')
parser.add_argument('--profile', type=str, help='Name of the AWS profile to use')
parser.add_argument('--region', type=str, help='Name of the AWS region to use')
parser.add_argument('--batch-size', type=int, default=25, help='Maximum batch size for DynamoDB')

args = parser.parse_args()

dynamodb_args = {}
if args.profile is not None:
    session = boto3.Session(profile_name=args.profile)
    dynamodb_args['region_name'] = args.region
    dynamodb = session.resource('dynamodb', **dynamodb_args)
else:
    dynamodb = boto3.resource('dynamodb', region_name=args.region)

table = dynamodb.Table(args.table_name)

def upload_json_to_dynamodb():
    with open(args.json_file_path) as f:
        data = json.load(f)

    batches = chunk_array(data, args.batch_size)

    for i, batch in enumerate(batches):
        with table.batch_writer() as batch_writer:
            for item in batch:
                batch_writer.put_item(Item=item)

        print(f"Uploaded batch {i + 1}/{len(batches)}")

    print("All batches uploaded successfully")

def chunk_array(arr, chunk_size):
    return [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]

upload_json_to_dynamodb()
