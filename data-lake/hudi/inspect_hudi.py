
import os
import boto3

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
)

response = client.get_object(
    Bucket="smartgrid-lake",
    Key="silver/mesures_hudi/.hoodie/hoodie.properties"
)

content = response["Body"].read().decode("utf-8")

for line in content.splitlines():
    if any(word in line.lower() for word in (
        "recordkey", "precombine", "partition", "table.name",
        "table.type", "keygenerator"
    )):
        print(line)
