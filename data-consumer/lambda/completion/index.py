"""
Crawler Completion Handler Lambda
Adds partition projection to tables after crawler completes.
"""
import boto3
import json
import os
from datetime import datetime

glue = boto3.client('glue')
s3 = boto3.client('s3')
sns = boto3.client('sns')


def get_oldest_partition_date(s3_location):
    """
    Get the oldest date partition from S3.
    Lists date= prefixes - S3 returns them in lexicographic order,
    so the first result is the oldest date.
    """
    try:
        parts = s3_location.replace('s3://', '').split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix.rstrip('/') + '/date=',
            Delimiter='/',
            MaxKeys=1,
            RequestPayer='requester'
        )
        
        for p in response.get('CommonPrefixes', []):
            folder = p['Prefix'].rstrip('/').split('/')[-1]
            if folder.startswith('date='):
                return folder.split('=')[1]
        return None
    except Exception as e:
        print(f"Error detecting oldest partition for {s3_location}: {e}")
        return None


def dedupe_table_schema(database_name, table_name):
    """
    Remove columns that collide with partition keys.
    Glue sometimes creates duplicate columns for partition keys.
    """
    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
        table = response['Table']
        
        partition_keys = {pk['Name'].lower() for pk in table.get('PartitionKeys', [])}
        if not partition_keys:
            return None
        
        sd = table['StorageDescriptor']
        original_cols = sd.get('Columns', [])
        filtered_cols = [c for c in original_cols if c['Name'].lower() not in partition_keys]
        removed = [c['Name'] for c in original_cols if c['Name'].lower() in partition_keys]
        
        if not removed:
            return None
        
        # Build TableInput with filtered columns
        table_input = {
            'Name': table['Name'],
            'StorageDescriptor': {**sd, 'Columns': filtered_cols},
        }
        for key in ['Description', 'Owner', 'Retention', 'PartitionKeys', 'TableType', 'Parameters']:
            if key in table:
                table_input[key] = table[key]
        
        glue.update_table(DatabaseName=database_name, TableInput=table_input)
        print(f"Deduped {database_name}.{table_name}: removed {removed}")
        return removed
    except Exception as e:
        print(f"Error deduping {database_name}.{table_name}: {e}")
        return None


def add_partition_projection(database_name, table_name):
    """
    Add partition projection properties to enable automatic partition discovery.
    This eliminates the need for ongoing crawler runs.
    """
    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
        table = response['Table']
        
        # Check for date partition key
        partition_keys = [pk['Name'].lower() for pk in table.get('PartitionKeys', [])]
        if 'date' not in partition_keys:
            print(f"Table {database_name}.{table_name} has no date partition, skipping")
            return None
        
        # Check if already enabled
        params = table.get('Parameters', {})
        if params.get('projection.enabled') == 'true':
            print(f"Projection already enabled for {database_name}.{table_name}")
            return None
        
        # Get S3 location and detect oldest partition
        location = table['StorageDescriptor']['Location'].rstrip('/')
        start_date = get_oldest_partition_date(location)
        
        if not start_date:
            print(f"Could not detect oldest partition for {database_name}.{table_name}, using 2020-01-01")
            start_date = '2020-01-01'
        
        # Add partition projection parameters
        new_params = {
            **params,
            'projection.enabled': 'true',
            'projection.date.type': 'date',
            'projection.date.range': f'{start_date},NOW',
            'projection.date.format': 'yyyy-MM-dd',
            'projection.date.interval': '1',
            'projection.date.interval.unit': 'DAYS',
            'storage.location.template': f'{location}/date=${{date}}'
        }
        
        # Build TableInput
        table_input = {
            'Name': table['Name'],
            'StorageDescriptor': table['StorageDescriptor'],
            'Parameters': new_params,
        }
        for key in ['Description', 'Owner', 'Retention', 'PartitionKeys', 'TableType']:
            if key in table:
                table_input[key] = table[key]
        
        glue.update_table(DatabaseName=database_name, TableInput=table_input)
        print(f"Added projection to {database_name}.{table_name} (start: {start_date})")
        return {'start_date': start_date, 'template': new_params['storage.location.template']}
    except Exception as e:
        print(f"Error adding projection to {database_name}.{table_name}: {e}")
        return None


def handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    crawler_name = event['detail']['crawlerName']
    state = event['detail']['state']
    
    if state != 'Succeeded':
        print(f"Crawler {crawler_name} did not succeed: {state}")
        return {'statusCode': 200, 'body': 'Skipped - crawler did not succeed'}
    
    # Get crawler details
    crawler = glue.get_crawler(Name=crawler_name)
    database_name = crawler['Crawler']['DatabaseName']
    
    # Get all tables in the database
    tables = []
    paginator = glue.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        tables.extend(page.get('TableList', []))
    
    print(f"Processing {len(tables)} tables in {database_name}")
    
    # Process each table
    schema_updates = {}
    projection_updates = {}
    
    for table in tables:
        table_name = table['Name']
        
        # Remove duplicate columns
        removed = dedupe_table_schema(database_name, table_name)
        if removed:
            schema_updates[table_name] = removed
        
        # Add partition projection
        projection = add_partition_projection(database_name, table_name)
        if projection:
            projection_updates[table_name] = projection
    
    # Build notification message
    message_parts = [
        "Crawler Completion Report",
        "=" * 26,
        f"Crawler: {crawler_name}",
        f"Database: {database_name}",
        f"Time: {datetime.now().isoformat()}",
        f"Tables: {len(tables)}",
        ""
    ]
    
    for table in tables:
        name = table['Name']
        cols = len(table['StorageDescriptor']['Columns'])
        if name in schema_updates:
            cols -= len(schema_updates[name])
        loc = table['StorageDescriptor']['Location']
        
        message_parts.append(f"  • {name}")
        message_parts.append(f"    Columns: {cols}")
        message_parts.append(f"    Location: {loc}")
        if name in projection_updates:
            message_parts.append(f"    Projection: ENABLED (from {projection_updates[name]['start_date']})")
        message_parts.append("")
    
    if projection_updates:
        message_parts.append("Partition projection enabled - crawlers no longer needed for new partitions.")
    
    message = "\n".join(message_parts)
    
    # Send notification
    sns.publish(
        TopicArn=os.environ['SNS_TOPIC_ARN'],
        Subject=f"Crawler Complete: {crawler_name}",
        Message=message
    )
    
    print(f"Notification sent for {crawler_name}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'database': database_name,
            'tables': len(tables),
            'projections_added': list(projection_updates.keys())
        })
    }
