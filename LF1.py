import json
import boto3
import os
import email
from email.parser import BytesParser
from email import policy
from hashlib import md5
import numpy as np
import sys
import re


if sys.version_info < (3,):
    maketrans = string.maketrans
else:
    maketrans = str.maketrans
    
def query_S3(bucket, objkey):

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    body = "";
    for obj in bucket.objects.all():
        key = obj.key
        if key==objkey:
            body = obj.get()['Body'].read()
    #print(body)
    raw_email = body
    msg = BytesParser(policy=policy.SMTP).parsebytes(body)


        # get the plain text version of the email
    plain = ''
    try:
        plain = msg.get_body(preferencelist=('plain'))
        plain = ''.join(plain.get_content().splitlines(keepends=True))
        plain = '' if plain == None else plain
    except:
        print('Incoming message does not have an plain text part - skipping this part.')


    #print("This is the plaintext : ",plain)

    return plain

def vectorize_sequences(sequences, vocabulary_length):
    results = np.zeros((len(sequences), vocabulary_length))
    for i, sequence in enumerate(sequences):
       results[i, sequence] = 1.
    return results

def one_hot_encode(messages, vocabulary_length):
    data = []
    for msg in messages:
        temp = one_hot(msg, vocabulary_length)
        data.append(temp)
    return data

def text_to_word_sequence(text,
                          filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                          lower=True, split=" "):

    if lower:
        text = text.lower()

    if sys.version_info < (3,):
        if isinstance(text, unicode):
            translate_map = dict((ord(c), unicode(split)) for c in filters)
            text = text.translate(translate_map)
        elif len(split) == 1:
            translate_map = maketrans(filters, split * len(filters))
            text = text.translate(translate_map)
        else:
            for c in filters:
                text = text.replace(c, split)
    else:
        translate_dict = dict((c, split) for c in filters)
        translate_map = maketrans(translate_dict)
        text = text.translate(translate_map)

    seq = text.split(split)
    return [i for i in seq if i]

def one_hot(text, n,
            filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
            lower=True,
            split=' '):

    return hashing_trick(text, n,
                         hash_function='md5',
                         filters=filters,
                         lower=lower,
                         split=split)


def hashing_trick(text, n,
                  hash_function=None,
                  filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                  lower=True,
                  split=' '):

    if hash_function is None:
        hash_function = hash
    elif hash_function == 'md5':
        hash_function = lambda w: int(md5(w.encode()).hexdigest(), 16)

    seq = text_to_word_sequence(text,
                                filters=filters,
                                lower=lower,
                                split=split)
    return [int(hash_function(w) % (n - 1) + 1) for w in seq]
    
def hit_sagemaker(text):
    """
        Hit sagemaker endpoint with text and get response. Return the confidence and other information.
    """
    endpoint_name = 'sms-spam-classifier-mxnet-2020-05-15-10-51-23-952'
    runtime = boto3.Session().client(service_name='sagemaker-runtime',region_name='us-east-1')
    one_hot_test_messages = one_hot_encode(text, 9013)
    encoded_test_messages = vectorize_sequences(one_hot_test_messages, 9013)
    #print (encoded_test_messages.tolist())
    payload = json.dumps(encoded_test_messages.tolist())
    #print (payload)
    response = runtime.invoke_endpoint(EndpointName=endpoint_name, ContentType='application/json', Body=payload)
    resp = json.loads(response['Body'].read().decode())
    #print (resp)
    return resp

def get_email(bucket,fileName):
    s3 = boto3.client("s3")
    file = s3.get_object(Bucket = bucket, Key = fileName)
    content = file["Body"].read().decode('utf-8')
    subject = content.split('Subject: ')[1]
    subject = subject.split('\n')[0]

    edate = content.split('Date: ')[1]
    edate = edate.split('\n')[0]

    sender = content.split('Return-Path: <')[1]
    sender = sender.split('>')[0]
    sender.split()
    email = []
    email.append(subject)
    email.append(edate)
    email.append(sender)
    print (subject,edate,sender)
    return email
def send_email(label,score,email):
    edate = email[1]
    subject = email[0]
    cbody = email[3]
    sender = email[2]
    ses = boto3.client('ses')
    body = """
    We received your email sent at {}  with the
    subject {}.
    Here is a 240 character sample of the email body:
    {} .
    The email was categorized as {} with a {}% confidence
    """.format(edate,subject,cbody,label,score)

    message = {"Subject" : {"Data" : subject}, "Body" : {"Html":{"Data": body}}}
    sender_email = 'help@harshul.tech'
    resp = ses.send_email(Source = sender_email, Destination = {"ToAddresses":[sender]},Message = message)


def cleanup(text):
    text = text.replace('\n', ' ').replace('\r', '')
    return text

def lambda_handler(event, context):
    # TODO implement
    #print (event)
    #event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-05-12T20:50:27.641Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'AWS:AIDAIE26RTG3F45XIHQFI'}, 'requestParameters': {'sourceIPAddress': '10.123.229.19'}, 'responseElements': {'x-amz-request-id': 'D8A0C083E35E2B9B', 'x-amz-id-2': 'tL8Co1dL6m3U+q2N4LKrq+Roiyi7lYa4rauSXnFBQ53HEnv+P+8p5g1XAjx9fSOodvkvqvYqtvzrmCZ7t49XxgDO0amZMba5'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': '50d785ae-de56-4a5b-ab56-df89f489271a', 'bucket': {'name': 'ses-test-domain', 'ownerIdentity': {'principalId': 'A10JJTOXS7BE7Q'}, 'arn': 'arn:aws:s3:::ses-test-domain'}, 'object': {'key': 'cevh6j43orscmun5elp4qg6im99hr67kkq804f01', 'size': 4094, 'eTag': '555f4fbacd3c3045d84ecbab60054893', 'sequencer': '005EBB0C198B2FDC25'}}}]}

    s3 = boto3.client("s3")
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    objkey = event["Records"][0]["s3"]["object"]["key"]

    file_obj = event["Records"][0]
    filename = str(file_obj["s3"]['object']['key'])
    #print("filename: ", filename)
    fileObj = s3.get_object(Bucket = bucket, Key=filename)
    #print("file has been gotten!")
    msg = email.message_from_bytes(fileObj['Body'].read())
    #print(msg)

    email1 = get_email(bucket,filename)
    text = query_S3(bucket, objkey)
    text = cleanup(text)
    email1.append(text)
    #print (text)
    response = hit_sagemaker(text)
    label = response.get("predicted_label")
    label = int(label[0][0])
    if label == 0:
        label1='HAM'
    elif label==1:
        label1='SPAM'
    score = response.get("predicted_probability")
    score = score[0][0]*100
    score = round(score,2)
    print (label1,score)
    send_email(label1,score,email1)
    # finalmessage = process_response(response)
    # send_email(email, finalmessage)
    #print (response)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
