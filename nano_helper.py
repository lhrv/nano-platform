import boto3
import csv
from io import StringIO
import pandas as pd
from io import BytesIO

BUCKET_PREFIX = "nano-platform-api"

def read_dataset(dataset_id):
    client = boto3.client("s3")
    response = client.get_object(Bucket=BUCKET_PREFIX + '-datasets', Key=dataset_id)
    csv_stream = response["Body"].read().decode("utf-8")
    string = StringIO(csv_stream)
    csv_dataset = csv.reader(string)
    return list(csv_dataset)

def read_raw_dataset(dataset_id):
    client = boto3.client("s3")
    response = client.get_object(Bucket=BUCKET_PREFIX + '-datasets', Key=dataset_id)
    csv_stream = response["Body"].read().decode("utf-8")
    string = StringIO(csv_stream)
    csv_dataset = csv.reader(string)
    return csv_dataset

def read_csv_to_dataframe(filename):
    s3 = boto3.resource('s3')
    obj = s3.Object(BUCKET_PREFIX + '-datasets', filename)
    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_csv(bio)
    return df

def read_json_to_dataframe(filename):
    s3 = boto3.resource('s3')
    obj = s3.Object(BUCKET_PREFIX + '-datasets', filename)
    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_json(bio)
    return df

def write_dataset(dataset_id, dataset):
    client = boto3.client("s3")
    string = StringIO()
    csv_writer = csv.writer(string)
    csv_writer.writerows(dataset)
    client.put_object(Bucket=BUCKET_PREFIX + '-datasets', Key=dataset_id, Body=string.getvalue())

def write_csv_dataframe(dataset_id, dataframe):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(BUCKET_PREFIX + '-datasets', dataset_id).put(Body=csv_buffer.getvalue())