"""AWS functions helper."""

import io
import zipfile
import requests
import boto3

LAYERS_ARN_LIST = ["arn:aws:lambda:us-east-1:123456789012:layer:layer-name:1"]
LAMBDA_SERVICE_ROLE_ARN = "arn:aws:iam::123456789012:role/lambda-service-role"
BUCKET_PREFIX = "nano-platform-api"

def deploy_lambda(name):
    """Create a lambda function."""
    # list all functions
    client = boto3.client('lambda')
    response = client.list_functions()
    base_name = name.split('.')[0]

    # If the <name> lambda does not exist, create the function.
    if name not in [function['FunctionName'] for function in response['Functions']]:
        response = client.create_function(
            FunctionName=base_name,
            Runtime='python3.9',
            Role=LAMBDA_SERVICE_ROLE_ARN,
            Handler=base_name + '.nano_function',
            Code={
                'ZipFile': open(name + ".zip", 'rb').read()
            },
            Description=f'{name} function description and libraries used',
            Timeout=60,
            MemorySize=512,
            Publish=True,
            Architectures=['x86_64'],
            Layers= LAYERS_ARN_LIST
        )
        return response

def is_lambda_exists(name):
    """Check if a lambda function exists."""
    client = boto3.client('lambda')
    response = client.list_functions()
    return name in [function['FunctionName'] for function in response['Functions']]

def invoke_function(name):
    """Invoke a function if it does exist."""
    if is_lambda_exists(name):
        client = boto3.client('lambda')
        response = client.invoke(
            FunctionName=name,
            InvocationType='RequestResponse',
            Payload=b'{"a": 1, "b": 2}'
        )
        return response
    else:
        return None

def get_function_code(id):
    """Get the url of the lambda code."""
    client = boto3.client('lambda')
    response = client.get_function(FunctionName=id)
    # Download the code zip file
    url = response['Code']['Location']
    response = requests.get(url)
    # Unzip the file and get the code
    with zipfile.ZipFile(io.BytesIO(response.content)) as myzip:
        with myzip.open(id + '.py') as myfile:
            return myfile.read().decode('utf-8')

# Check if the S3 file exists
def is_dataset_exist(dataset_filename):
    """Check if a dataset exists on S3."""
    aws_resource = boto3.resource('s3')
    try:
        aws_resource.Object(BUCKET_PREFIX + '-datasets', dataset_filename).load()
    except Exception as exception:
        return False
    return True

def create_empty_dataset(dataset_filename):
    """Create an empty dataset on S3."""
    aws_resource = boto3.resource('s3')
    response = aws_resource.Object(BUCKET_PREFIX + '-datasets', dataset_filename).put()
    return response


def upload_dataset(dataset_filename):
    """Upload dataset to S3."""
    aws_resource = boto3.resource('s3')
    response = aws_resource.meta.client.upload_file(
        dataset_filename, BUCKET_PREFIX + '-datasets', dataset_filename
    )
    return response

def read_dataset(dataset_filename):
    """Read dataset from S3."""
    aws_resource = boto3.resource('s3')
    response = aws_resource.Object(BUCKET_PREFIX + '-datasets', dataset_filename).get()['Body'].read()
    return response

def paginate_dataset(dataset_filename, index, size):
    """Paginate dataset from S3."""
    client = boto3.client('s3')
    end_record = index + size

    resp = client.select_object_content(
        Bucket = BUCKET_PREFIX + '-datasets',
        Key = dataset_filename,
        Expression = f"""select * from S3Object s where cast(s.id as int)>{index} and cast(s.id as int)<{end_record}""",
        ExpressionType = 'SQL',
        InputSerialization = {'CSV': {'FileHeaderInfo': 'Use'}},
        OutputSerialization = {'CSV': {}}

    )
    result = []
    for row in resp['Payload']:
        if 'Records' in row:
            result.append(row['Records']['Payload'].decode('utf-8'))
    return result

# Upload page to S3
def upload_page(page_filename):
    """Upload page to S3."""
    aws_resource = boto3.resource('s3')
    response = aws_resource.meta.client.upload_file(
        page_filename, BUCKET_PREFIX + '-pages', page_filename
    )
    return response

# Read page from S3
def read_page(page_filename):
    """Read page from S3."""
    aws_resource = boto3.resource('s3')
    response = aws_resource.Object(BUCKET_PREFIX + '-pages', page_filename).get()['Body'].read()
    return response

def get_page(page_filename):
    """Get page from S3."""
    aws_resource = boto3.resource('s3')
    response = aws_resource.Object(BUCKET_PREFIX + '-pages', page_filename).get()
    return response

def is_page_exist(page_filename):
    """Check if a page exists on S3."""
    aws_resource = boto3.resource('s3')
    try:
        aws_resource.Object(BUCKET_PREFIX + '-pages', page_filename).load()
    except Exception as e:
        return False
    return True