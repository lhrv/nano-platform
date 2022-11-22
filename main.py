"""Main file for the API project."""

import json
import os
import zipfile
import boto3
import aws
from fastapi import FastAPI, UploadFile, HTTPException
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import Terminal256Formatter
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

BUCKET_PREFIX = "nano-platform-api"

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#############
# Datasets
#############

# Create a dataset resource on S3 and upload file
@app.post("/datasets/", status_code=201)
async def create_dataset(file: UploadFile):
    """POST endpoint to create a dataset."""
    # Check if the file is uploaded
    if file is None:
        return {"message": "No file uploaded"}
    # Check if dataset already exist in S3
    if aws.is_dataset_exist(file.filename):
        raise HTTPException(status_code=409, detail="Item already exists.")
    # Write the file to the disk
    with open(file.filename, "wb") as buffer:
        buffer.write(file.file.read())
    print(f"{file.filename} uploaded.")

    # Upload the file to S3
    aws.upload_dataset(file.filename)
    # Delete the file from the disk
    os.remove(file.filename)
    return {"message": f"File {file.filename} uploaded"}

# Create an empty dataset resource on S3
@app.post("/datasets/empty/{name}")
async def create_empty_dataset(name: str):
    """POST endpoint to create an empty dataset."""
    # Check if dataset already exists in S3, if it exists, return an error
    if aws.is_dataset_exist(name):
        return {"message": f"Dataset name <{name}> already taken."}

    # Upload an empty file to AWS S3
    client = boto3.client('s3')
    response = client.put_object(
        Body=b'',
        Bucket=BUCKET_PREFIX + '-datasets',
        Key=name
    )
    return {
        "SystemResponse": response,
        "DatasetResponse": {"message": f"Empty dataset <{name}> created."}
    }

# Update a dataset and upload it to S3
@app.put("/datasets/")
async def update_existing_dataset(file: UploadFile):
    """PUT endpoint to update dataset to S3 bucket."""
    # Check if the file is uploaded
    if file is None:
        return {"message": "No file uploaded"}
    # Check if dataset already exist in S3
    if not aws.is_dataset_exist(file.filename):
        return {"message": f"Dataset <{file.filename}> not found."}

    # Write the file to the disk
    with open(file.filename, "wb") as buffer:
        buffer.write(file.file.read())

    # Upload the file to S3
    aws.upload_dataset(file.filename)
    # Delete the file from the disk
    os.remove(file.filename)
    return {"message": f"File <{file.filename}> updated."}

# List all datasets
@app.get("/datasets/")
async def list_datasets(detailed: bool = False):
    """GET endpoint to list all datasets available to the users."""
    client = boto3.client('s3')
    # if bucket list is empty, return an error
    bucket_list = client.list_objects(Bucket=BUCKET_PREFIX + '-datasets')
    if "Contents" not in bucket_list:
        return {"message": "No datasets available."}
    # if detailed is True, return all metadata
    if detailed:
        return bucket_list
    # Select only keys, creation dates, and size.
    keys = [obj['Key'] for obj in bucket_list['Contents']]
    dates = [obj['LastModified'] for obj in bucket_list['Contents']]
    sizes = [obj['Size'] for obj in bucket_list['Contents']]
    response = []

    for i, _ in enumerate(keys):
        response.append(
            {"dataset_id": keys[i],
            "creation_date": dates[i],
            "file_size": sizes[i]})
    return response

# Download the dataset
@app.get("/datasets/{id}")
async def download_dataset(id: str, raw_file: bool = False):
    """GET endpoint to download a dataset."""
    # Check if id dataset exists in S3
    if not aws.is_dataset_exist(id):
        return {"message": "Dataset not found"}

    client = boto3.client('s3')
    response = client.get_object(Bucket=BUCKET_PREFIX + '-datasets', Key=id)
    csv_stream = response["Body"].read().decode("utf-8")
    if raw_file:
        return HTMLResponse(content=csv_stream, media_type="text/csv")
    # Method 2: StreamingResponse
    # return StreamingResponse(io.StringIO(csv_stream), media_type="text/csv")

    return {
        "SystemResponse": response,
        "DatasetResponse": csv_stream
    }

@app.get("/datasets/{id}/page/{page}")
async def download_dataset(id: str, page: int, raw_file: bool = False):
    """GET endpoint to paginate a dataset."""
    # Check if id dataset exists in S3
    if not aws.is_dataset_exist(id):
        return {"message": "Dataset not found"}

    csv_stream = aws.paginate_dataset(id, page, 20)
    if raw_file:
        return HTMLResponse(content=csv_stream, media_type="text/csv")
    # Method 2: StreamingResponse
    # return StreamingResponse(io.StringIO(csv_stream), media_type="text/csv")

    return {
        "SystemResponse": None,
        "DatasetResponse": csv_stream
    }

# Delete the dataset
@app.delete("/datasets/{id}", status_code=204)
async def delete_dataset(id: str):
    """DELETE endpoint to delete a dataset."""
    client = boto3.client('s3')
    if not aws.is_dataset_exist(id):
        return
    client.delete_object(Bucket=BUCKET_PREFIX + '-datasets', Key=id)

#############
# Functions
#############

@app.post("/functions/")
async def create_upload_code_file(file: UploadFile):
    """POST endpoint to upload *.py files to AWS Lambda."""

    # Write the file to the disk
    with open(file.filename, "wb") as buffer:
        buffer.write(file.file.read())
    print(f"{file.filename} uploaded")

    # Zip the file
    with zipfile.ZipFile(file.filename + ".zip", 'w') as myzip:
        myzip.write(file.filename, file.filename)
        myzip.write("nano_helper.py")
    
    # Create the lambda function
    aws.deploy_lambda(file.filename)
    # # Delete the file from the disk
    os.remove(file.filename)
    os.remove(file.filename + ".zip")
    return {"message": "File uploaded"}

# list all functions
@app.get("/functions/")
async def list_functions():
    """GET endpoint to list all functions available to the users."""
    client = boto3.client('lambda')
    response = client.list_functions()
    return response

# Call the function and return the result
@app.get("/functions/{id}")
async def invoke_function(id: str):
    """GET endpoint to invoke a function and return the result."""
    response = aws.invoke_function(id)
    if response is None:
        system_response = {"message": "Function not found"}
        function_response = None
    else:
        system_response = response
        function_response = json.load(response['Payload'])

    return {
        "SystemResponse": system_response,
        "FunctionResponse": function_response
    }

# Delete the function
@app.delete("/functions/{id}")
async def delete_function(id: str):
    """DELETE endpoint to delete a function."""
    # Check if id lambda exists in AWS

    client = boto3.client('lambda')
    response = client.delete_function(FunctionName=id)
    return response

# Display the function code
# with query param True/false option to color the code.
@app.get("/functions/{id}/code/")
async def get_function_code(id: str, color: bool = False):
    """GET endpoint to display the code of a function."""
    response = aws.get_function_code(id)
    if response is None:
        system_response = {"message": "Function not found"}
        function_response = None
    else:
        system_response = {"message": "Function code unzipped successfully"}
        function_response = response
    if color:
        function_response = highlight(function_response, PythonLexer(), Terminal256Formatter())
    print(color)
    return {
        "SystemResponse": system_response,
        "FunctionResponse": function_response
    }

#############
# Pages
#############

# Create a page (yaml file)
@app.post("/pages/")
async def create_page(file: UploadFile):
    """POST endpoint to upload *.yaml files to AWS S3."""
    # Write the file to the disk
    with open(file.filename, "wb") as buffer:
        buffer.write(file.file.read())
    print(f"{file.filename} uploaded")
    # Upload the file to S3
    aws.upload_page(file.filename)
    # Delete the file from the disk
    os.remove(file.filename)
    return {"message": "Page file uploaded"}

# List all pages
@app.get("/pages/")
async def list_pages(detailed: bool = False):
    """GET endpoint to list all pages available to the users."""
    client = boto3.client('s3')
    bucket_list= client.list_objects(Bucket=BUCKET_PREFIX + '-pages')

    if "Contents" not in bucket_list:
        return {"message": "No pages available."}
    # if detailed is True, return all metadata
    if detailed:
        return bucket_list
    # Select only keys, creation dates, and size.
    keys = [obj['Key'] for obj in bucket_list['Contents']]
    dates = [obj['LastModified'] for obj in bucket_list['Contents']]
    sizes = [obj['Size'] for obj in bucket_list['Contents']]
    response = []
    # for loop using enumerate:
    for i, _ in enumerate(keys):
        response.append(
            {"dataset_id": keys[i],
            "creation_date": dates[i],
            "file_size": sizes[i]})
    return response

# Download the page
@app.get("/pages/{id}")
async def download_page(id: str, raw_file: bool = False):
    """GET endpoint to download a page."""
    # Check if id page exists in S3
    if not aws.is_page_exist(id):
        return {"message": "Page not found"}

    client = boto3.client('s3')
    response = client.get_object(Bucket=BUCKET_PREFIX + '-pages', Key=id)
    csv_stream = response["Body"].read().decode("utf-8")
    if raw_file:
        return HTMLResponse(content=csv_stream, media_type="text/yaml")
    # Method 2: StreamingResponse
    # return StreamingResponse(io.StringIO(csv_stream), media_type="text/csv")

    return {
        "SystemResponse": response,
        "DatasetResponse": csv_stream
    }