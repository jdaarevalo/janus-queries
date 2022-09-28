"""
Purpose
Run an Athena query and post in Slack the output location as CSV
Tag the user who does the request
"""

import os
import json
import time
import boto3
import urllib3
import ast


def lambda_handler(event, context):
    # Create the Athena client with boto3
    athena_client = boto3.client('athena')
    
    # Fetch the query, here you can use a named_query_id or directly the query with params
    query = get_query()

    # Execute the Athena query
    query_execution_id = execute_query(athena_client, query)
    
    # Fetch the location in Amazon s3 with the results
    s3_uri_output_location = query_execution_output_location(athena_client, query_execution_id)
    
    # Fetch the HTTP link
    report_http_link = get_presigned_http_link(s3_uri_output_location)
    
    # Send a message to slack
    send_slack_message(event, report_http_link)
    
def get_query():
    """
    :return: query string
    :rtype: str
    """

    query = "SELECT * FROM raw_data_covid_19.covid_nytimes_states limit 10;"
    return query

def execute_query(athena_client, query):
    """
    Return the query_execution_id
    
    :param athena_client: The boto3 Athena client
    :type athena_client: boto3.client
    :param query: query to execute in Athena
    :type query: str
    :return: query_execution_id
    :rtype: str
    """
    output_location = 's3://{bucket_name}/'.format(bucket_name=os.environ.get("BUCKET_NAME"))
    
    query_execution = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={ 'Database': os.environ.get("ATHENA_DATABASE")},
        ResultConfiguration={ 'OutputLocation': output_location }
    )

    return query_execution["QueryExecutionId"]
  
def query_execution_output_location(athena_client, query_execution_id):
    """
    Return the output location of a query_execution, after validating the query is finished
    every 2 secs until the query_execution state is 'SUCCEEDED' or after 60 validations.
    
    :param athena_client: The boto3 Athena client
    :type athena_client: boto3.client
    :param query_execution_id: The query_execution_id
    :type query_execution_id: str
    :return:  location in Amazon S3 where query results are stored
    :rtype: str
    """
    query_execution_state = 'RUNNING'
    max_execution = 60
    
    while (max_execution > 0 and query_execution_state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        query_execution_response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )["QueryExecution"]
            
        query_execution_state = query_execution_response["Status"]["State"]
        
        if query_execution_state == 'FAILED':
            return False
        elif query_execution_state == 'SUCCEEDED':
            return query_execution_response["ResultConfiguration"]["OutputLocation"]
        time.sleep(2)
        

def get_presigned_http_link(s3_uri):
    """
    Generate a presigned URL for users who does not have permission to access 
    an S3 object
    
    :param s3_uri: location in Amazon S3
    :type s3_uri: str
    :return: http link
    :rtype: str
    """
    bucket_name = os.environ.get("BUCKET_NAME")
    secs_to_expire_report = os.environ.get("SECS_TO_EXPIRE_REPORT")
    
    s3_client = boto3.client('s3')
    
    object_name = s3_uri.split(bucket_name +'/')[1]
    
    return s3_client.generate_presigned_url('get_object',
                Params={'Bucket': bucket_name, 'Key': object_name},
                ExpiresIn=secs_to_expire_report)
    
def send_slack_message(event, report_http_link):
    """
    Send a slack message tagging the user who does the request, with the link to download the CSV report
    
    :param event: Lambda event
    :type event: dict
    :param report_http_link: presigned URL to download the report
    :type report_http_link: str
    :return: None
    """
    
    slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    http = urllib3.PoolManager()
    try:
        json_body_request = ast.literal_eval(event['Records'][0]['body'])
        tag_user = "<@{}>".format(json_body_request['user_id_request'])
    except:
        tag_user = " "
    data  = {
    	"blocks": [
    		{
    			"type": "section",
    			"text": {
    				"type": "mrkdwn",
    				"text": "Hey {} good news! your report has been generated".format(tag_user)
    			}
    		},
    		{
    			"type": "section",
    			"fields": [
    				{
    					"type": "mrkdwn",
    					"text": "*Type:*\nCSV"
    				},
    				{
    					"type": "mrkdwn",
    					"text": "*Download here:*\n<{}|report.csv>".format(report_http_link)
    				}
    			]
    		}
    	]
    }
    
    http.request("POST", 
            slack_webhook_url,
            body = json.dumps(data),
            headers = {"Content-Type": "application/json"})