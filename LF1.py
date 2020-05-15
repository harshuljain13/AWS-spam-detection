from __future__ import print_function

import boto3
import json
import os
from sms_spam_classifier_utilities import one_hot_encode
from sms_spam_classifier_utilities import vectorize_sequences
from datetime import datetime

# Update the emailDomain environment variable to the correct domain, e.g. <MYDOMAIN>.com
EMAIL_DOMAIN = os.environ['emailDomain']


def lambda_handler(event, context):
    s3_info = event['Records'][0]['s3']
    bucket_name = s3_info['bucket']['name']
    key_name = s3_info['object']['key']
    
    s3 = boto3.client('s3')
    ses = boto3.client('ses')
    data = s3.get_object(Bucket=bucket_name, Key=key_name)
    content = data['Body'].read().decode('utf-8')
    
    cbody = content.split('Content-Type: text/plain; charset="UTF-8"')[1]
    cbody = cbody.split('Content-Type: text/html; charset="UTF-8"')[0]
    cbody = cbody.rsplit("\n",3)[0]
    cbody = cbody.replace("\n"," ")
    print(cbody)
    subject = content.split('Subject: ')[1]
    subject = subject.split('\n')[0]

    edate = content.split('Date: ')[1]
    edate = edate.split('\n')[0]
        
    sender = content.split('Return-Path: <')[1]
    sender = sender.split('>')[0]
    
    endpoint_name = 'sms-spam-classifier-mxnet-2020-05-15-10-51-23-952'
    runtime = boto3.Session().client(service_name='sagemaker-runtime',region_name='us-east-1')
        
    payload = [cbody]
    
    response = runtime.invoke_endpoint(EndpointName=endpoint_name, Body=json.dumps(payload))
    result = response['Body'].read()
    
    print(result)
    

    body = """
        We received your email sent at {}  with the
        subject {}.
        Here is a 240 character sample of the email body:
        {} .
        The email was categorized as {} with a {}% confidence
        """.format(edate,subject,cbody,result[0], result[1])
        
    message = {"Subject" : {"Data" : subject}, "Body" : {"Html":{"Data": body}}}
        
    resp = ses.send_email(Source = "help@harshul.tech", Destination = {"ToAddresses":[sender]},Message = message)
        
    return "Done"
