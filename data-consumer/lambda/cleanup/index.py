"""
Initial Discovery Trigger / Cleanup Lambda
Triggers discovery on stack creation, cleans up on deletion.
"""
import boto3
import json
import time
import urllib.request

lambda_client = boto3.client('lambda')
glue = boto3.client('glue')


def send_cfn_response(event, context, status, data, physical_id):
    """Send response to CloudFormation."""
    body = json.dumps({
        'Status': status,
        'Reason': f'See CloudWatch Log Stream: {context.log_stream_name}',
        'PhysicalResourceId': physical_id,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Data': data
    })
    req = urllib.request.Request(
        event['ResponseURL'],
        data=body.encode('utf-8'),
        headers={'Content-Type': ''},
        method='PUT'
    )
    urllib.request.urlopen(req)


def cleanup_glue_resources(stack_name):
    """Delete all Glue crawlers and databases created by this stack."""
    # Delete crawlers with stack prefix
    for crawler_name in glue.list_crawlers().get('CrawlerNames', []):
        if crawler_name.startswith(f"{stack_name}-"):
            try:
                if glue.get_crawler(Name=crawler_name)['Crawler']['State'] == 'RUNNING':
                    glue.stop_crawler(Name=crawler_name)
                    time.sleep(5)
                glue.delete_crawler(Name=crawler_name)
            except Exception:
                pass

    # Delete databases tagged with stack name
    for db in glue.get_databases().get('DatabaseList', []):
        if f'[{stack_name}]' in db.get('Description', ''):
            try:
                for table in glue.get_tables(DatabaseName=db['Name']).get('TableList', []):
                    glue.delete_table(DatabaseName=db['Name'], Name=table['Name'])
                glue.delete_database(Name=db['Name'])
            except Exception:
                pass


def handler(event, context):
    print(f"Event: {json.dumps(event)}")
    try:
        if event['RequestType'] == 'Create':
            lambda_client.invoke(
                FunctionName=event['ResourceProperties']['DiscoveryFunctionName'],
                InvocationType='RequestResponse',
                Payload=json.dumps({})
            )
            send_cfn_response(event, context, 'SUCCESS', {'Result': 'OK'}, 'InitialDiscovery')
        elif event['RequestType'] == 'Delete':
            cleanup_glue_resources(event['ResourceProperties']['StackName'])
            send_cfn_response(event, context, 'SUCCESS', {'Result': 'Cleaned'}, 'InitialDiscovery')
        else:
            send_cfn_response(event, context, 'SUCCESS', {'Result': 'NoOp'}, 'InitialDiscovery')
    except Exception as e:
        print(f"Error: {e}")
        if event['RequestType'] == 'Delete':
            send_cfn_response(event, context, 'SUCCESS', {'Result': str(e)}, 'InitialDiscovery')
        else:
            send_cfn_response(event, context, 'FAILED', {'Error': str(e)}, 'InitialDiscovery')
