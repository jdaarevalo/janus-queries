"""
Purpose
Format params, trigger the SQS and finaly response to API Gateway, 
"""

import os
import base64
import boto3
import json
import time
from urllib import parse as urlparse
from functools import lru_cache

def lambda_handler(event, context):
    # data comes b64 and also urlencoded name=value& pairs
    message_map = dict(urlparse.parse_qsl(base64.b64decode(str(event['body'])).decode('ascii')))  
    # will be /command name that does the request
    command = message_map.get('command','err')
    # We can validate the command or the params 
    # params = message_map.get('text','err').split(" ")  # params ['some_uuid','']
    
    if command == '/generate-report':
        # slack user_id that does the request
        user_id = message_map.get('user_id','err')
        try:
            send_sqs_message(user_id)
            response_message = "Ok, I will start to work on your request, as soon as the query finish I will send you the report in the channel "
        except:
            response_message = "Mmm, Houston we have a problem! Please report the error to the Data Engineer team"
    else:
        response_message = """Sorry, this command has not been developed yet, please get in touch with the Data Engineer team"""
    
    return {
        'statusCode': 200,
        'body': json.dumps(response_message)
    }
    
def send_sqs_message(user_id):
    """
    Send the message to SQS
    :param user_id: The user_id that does the request on Slack.
    :type user_id: str
    :return: sqs_response
    """
    sqs_client = boto3.client('sqs')
    response = sqs_client.send_message(
        QueueUrl=os.environ.get("SQS_URL"),
        MessageBody=str({'user_id_request': user_id })
    )
    return response