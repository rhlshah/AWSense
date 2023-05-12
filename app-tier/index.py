import base64
from urllib import response
import boto3
from botocore.exceptions import ClientError
import os
import time
import datetime
import logging  
import io

aws_access_key_id =''
aws_secret_access_key = ''
region_name = 'us-east-1'
request_queue_url = ''
response_queue_url = ''
endpoint_url = 'https://sqs.us-east-1.amazonaws.com'
s3_input_bucket = ""
s3_output_bucket = ""
s3 = boto3.resource(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )
sqs = boto3.client('sqs', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url, region_name=region_name)
s3_client = boto3.client('s3', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

def pollForReqests() :
    print("Polling for messages:")
    try:
        response = sqs.receive_message(
            QueueUrl=request_queue_url,
                AttributeNames=[
                'SentTimestamp'
                ],
                MaxNumberOfMessages=10,
                MessageAttributeNames=[
                'All'
                ],
                VisibilityTimeout=30,
            )

    except Exception as e:
        print(str(e))
        return "Something went wrong"


    if 'Messages' in response :
        reciept_handle = response['Messages'][0]['ReceiptHandle']
        rr = response['Messages']

        print(rr)
        deleteMessageFromRequestQueue(reciept_handle)
        return rr
    else :
        time.sleep(1)
        return pollForReqests()

def deleteMessageFromRequestQueue(receipt_handle) :
    sqs.delete_message(
        QueueUrl = request_queue_url,
        ReceiptHandle = receipt_handle
    )

def decodeMessage(fName, msg) :
    decodeit=open(fName,'wb')
    decodeit.write(base64.b64decode((msg)))
    decodeit.close()

def sendMessageInResponseQueue(fName, msg) :
    endpoint_url = 'https://sqs.us-east-1.amazonaws.com'
    resp = sqs.send_message(
    QueueUrl = response_queue_url,
        MessageBody=(
        fName + " " + msg
        )
    
    )
    
def upload_to_s3_input_bucket(file_name, bucket, object_name) :
    try:
        response = s3_client.upload_fileobj(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_to_s3_output_bucket(s3, bucket_name, image_name, predicted_result) :
    content = (image_name, predicted_result)
    content = ' '.join(str(x) for x in content)
    s3.Object(s3_output_bucket, image_name).put(Body=content)

def initialize() :

    val = pollForReqests()
    
    if(val == None or len(val) == 0):
        print('Some error occured. No requests found')
        return
    
    message = val[0]
    
    fName , encodedMssg=message['Body'].split()
    justFName = fName
    fName = fName + ".jpeg"
    print('file name : ' + fName)

    msg_value = bytes(encodedMssg, 'ascii')
    qp = base64.b64decode(msg_value)
    print(qp)
    with open(fName, "wb") as file:
        file.write(qp)

    with open(fName, 'rb') as f:
        if upload_to_s3_input_bucket(f, s3_input_bucket, fName):
            print("uploaded image to S3 bucket")

    stdout = os.popen(f'python3 image_classification.py "{fName}"')
    result = stdout.read().strip()
    logging.info('result : ' + result)
    print("result " + result)

    with open(fName, 'rb') as f:
        upload_to_s3_output_bucket(s3, s3_output_bucket, justFName, result)
        sendMessageInResponseQueue(justFName, result)

    print(result)
    
    

logging.info('Timestamp : ' + str(datetime.datetime.now()))
while True :
    initialize()