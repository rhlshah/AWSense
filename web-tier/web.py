import os
import boto3
from flask import Flask, request, jsonify
import base64

app = Flask(__name__)
res = dict()
aws_access_key_id =''
aws_secret_access_key = ''
region_name = 'us-east-1'
request_queue_url = ''
response_queue_url = ''
endpoint_url = 'https://sqs.us-east-1.amazonaws.com'

s3 = boto3.resource(
        service_name='s3',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

sqs = boto3.resource('sqs', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url, region_name=region_name)
sqs_client = boto3.client('sqs', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url, region_name=region_name)


@app.route("/home")
def homePage():
    return "Welcome to the Home Page"

@app.route('/', methods=["GET", "POST"])
def populate_to_sqs_request_queue():
    cnt = 0
    output = None
    print(request.files)
    if 'myfile' in request.files:
        image = request.files['myfile']
        im = image.read()
        f_name = str(image).split(" ")[1][1:][:-1]
        
        print(f_name)
        if f_name != '':
            f_extension = os.path.splitext(f_name)[1]
            print(f_extension)
            #performing encoding
            byteform=base64.b64encode(im)
            value = str(byteform, 'ascii')
            str_byte=f_name.split('.')[0] + " " + value
            print(str_byte)
            resp = sqs_client.send_message(
                QueueUrl=request_queue_url,
                MessageBody=str_byte,
            )
            
            print(resp)
            print(f_name.split('.')[0])
            try:
                output  = get_response(f_name.split('.')[0])
                print("OUTPUT")
                print(output)
                data = {
                    "text" : output,
                }
                return jsonify(data)

            except Exception as e:
                print(str(e))
                return "Something went wrong! x"
        else :
            return "Error with file name"
    else:
        return "Please upload an image file"


def get_response(image) :
    result = ""
    #infinite loop to keep listening until response found
    while True:
        if image in res.keys():
            return res[image]
        response = sqs_client.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
        )

        if 'Messages' in response:
            msgs = response['Messages']
            for msg in msgs:
                msg_body = msg['Body']
                res_image = msg_body.split(" ")[0]
                # print("result image: ")
                # print(res_image.split(".")[0])

                res[res_image] = msg_body.split(" ")[1:]
                # print(res[res_image])
                receipt_handle = msg['ReceiptHandle']
                #deleting consumed message from the queue
                sqs_client.delete_message(
                    QueueUrl = response_queue_url,
                    ReceiptHandle = receipt_handle
                )
                
                if res_image.split(".")[0] == image :
                    return res[res_image]


if __name__ == "__main__":
    app.run(host=os.getenv('IP', '0.0.0.0'),
            port=int(os.getenv('PORT', 8081)))
