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
    deleted_crawlers = []
    deleted_databases = []
    errors = []
    
    # Delete crawlers with stack prefix
    try:
        crawlers = glue.list_crawlers().get('CrawlerNames', [])
        for crawler_name in crawlers:
            if crawler_name.startswith(f"{stack_name}-"):
                try:
                    # Stop if running
                    state = glue.get_crawler(Name=crawler_name)['Crawler']['State']
                    if state == 'RUNNING':
                        print(f"Stopping crawler: {crawler_name}")
                        glue.stop_crawler(Name=crawler_name)
                        time.sleep(5)
                    
                    print(f"Deleting crawler: {crawler_name}")
                    glue.delete_crawler(Name=crawler_name)
                    deleted_crawlers.append(crawler_name)
                except Exception as e:
                    errors.append(f"Error deleting crawler {crawler_name}: {e}")
                    print(errors[-1])
    except Exception as e:
        errors.append(f"Error listing crawlers: {e}")
        print(errors[-1])
    
    # Delete databases tagged with stack name
    try:
        databases = glue.get_databases().get('DatabaseList', [])
        for db in databases:
            db_name = db['Name']
            description = db.get('Description', '')
            
            # Only delete if tagged with our stack name
            if f'[{stack_name}]' in description:
                try:
                    # Delete tables first
                    tables = glue.get_tables(DatabaseName=db_name).get('TableList', [])
                    for table in tables:
                        print(f"Deleting table: {db_name}.{table['Name']}")
                        glue.delete_table(DatabaseName=db_name, Name=table['Name'])
                    
                    print(f"Deleting database: {db_name}")
                    glue.delete_database(Name=db_name)
                    deleted_databases.append(db_name)
                except Exception as e:
                    errors.append(f"Error deleting database {db_name}: {e}")
                    print(errors[-1])
    except Exception as e:
        errors.append(f"Error listing databases: {e}")
        print(errors[-1])
    
    return {
        'deleted_crawlers': deleted_crawlers,
        'deleted_databases': deleted_databases,
        'errors': errors
    }


def handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    request_type = event['RequestType']
    discovery_function = event['ResourceProperties']['DiscoveryFunctionName']
    stack_name = event['ResourceProperties']['StackName']
    
    try:
        if request_type == 'Create':
            # Invoke discovery on stack creation
            print(f"Invoking discovery function: {discovery_function}")
            response = lambda_client.invoke(
                FunctionName=discovery_function,
                InvocationType='RequestResponse',
                Payload=json.dumps({'trigger': 'initial_deployment'})
            )
            
            payload = json.loads(response['Payload'].read())
            print(f"Discovery response: {json.dumps(payload)}")
            
            send_cfn_response(event, context, 'SUCCESS', {'Result': 'Initial discovery completed'}, 'InitialDiscovery')
        
        elif request_type == 'Update':
            print("Stack update - skipping discovery")
            send_cfn_response(event, context, 'SUCCESS', {'Result': 'Update - no action'}, 'InitialDiscovery')
        
        elif request_type == 'Delete':
            print(f"Cleaning up Glue resources for stack: {stack_name}")
            result = cleanup_glue_resources(stack_name)
            print(f"Cleanup result: {json.dumps(result)}")
            
            send_cfn_response(event, context, 'SUCCESS', 
                {'Result': f"Deleted {len(result['deleted_crawlers'])} crawlers, {len(result['deleted_databases'])} databases"},
                'InitialDiscovery')
            
    except Exception as e:
        print(f"Error: {e}")
        # Always succeed on delete to not block stack deletion
        if request_type == 'Delete':
            send_cfn_response(event, context, 'SUCCESS', {'Result': f'Delete completed with errors: {str(e)}'}, 'InitialDiscovery')
        else:
            send_cfn_response(event, context, 'FAILED', {'Error': str(e)}, 'InitialDiscovery')
