"""
Crawler Completion Handler Lambda
Adds partition projection to tables after crawler completes.
"""
import boto3
import json
import os

glue = boto3.client('glue')
s3 = boto3.client('s3')
sns = boto3.client('sns')


def get_oldest_partition_date(s3_location):
    """Get the oldest date partition from S3 (lexicographic = chronological)."""
    try:
        parts = s3_location.replace('s3://', '').split('/', 1)
        response = s3.list_objects_v2(
            Bucket=parts[0],
            Prefix=parts[1].rstrip('/') + '/date=',
            Delimiter='/',
            MaxKeys=1,
            RequestPayer='requester'
        )
        for p in response.get('CommonPrefixes', []):
            folder = p['Prefix'].rstrip('/').split('/')[-1]
            if folder.startswith('date='):
                return folder.split('=')[1]
    except Exception:
        pass
    return None


def dedupe_table_schema(database_name, table_name):
    """Remove columns that collide with partition keys."""
    try:
        table = glue.get_table(DatabaseName=database_name, Name=table_name)['Table']
        partition_keys = {pk['Name'].lower() for pk in table.get('PartitionKeys', [])}
        if not partition_keys:
            return None

        sd = table['StorageDescriptor']
        original_cols = sd.get('Columns', [])
        filtered_cols = [c for c in original_cols if c['Name'].lower() not in partition_keys]
        removed = [c['Name'] for c in original_cols if c['Name'].lower() in partition_keys]
        if not removed:
            return None

        table_input = {'Name': table['Name'], 'StorageDescriptor': {**sd, 'Columns': filtered_cols}}
        for key in ['Description', 'Owner', 'Retention', 'PartitionKeys', 'TableType', 'Parameters']:
            if key in table:
                table_input[key] = table[key]
        glue.update_table(DatabaseName=database_name, TableInput=table_input)
        return removed
    except Exception:
        return None


def add_partition_projection(database_name, table_name):
    """Add partition projection properties to enable automatic partition discovery."""
    try:
        table = glue.get_table(DatabaseName=database_name, Name=table_name)['Table']
        if 'date' not in [pk['Name'].lower() for pk in table.get('PartitionKeys', [])]:
            return None

        params = table.get('Parameters', {})
        if params.get('projection.enabled') == 'true':
            return None

        location = table['StorageDescriptor']['Location'].rstrip('/')
        start_date = get_oldest_partition_date(location) or '2020-01-01'

        params.update({
            'projection.enabled': 'true',
            'projection.date.type': 'date',
            'projection.date.range': f'{start_date},NOW',
            'projection.date.format': 'yyyy-MM-dd',
            'projection.date.interval': '1',
            'projection.date.interval.unit': 'DAYS',
            'storage.location.template': f'{location}/date=${{date}}'
        })

        table_input = {
            'Name': table['Name'],
            'StorageDescriptor': table['StorageDescriptor'],
            'Parameters': params
        }
        for key in ['Description', 'Owner', 'Retention', 'PartitionKeys', 'TableType']:
            if key in table:
                table_input[key] = table[key]
        glue.update_table(DatabaseName=database_name, TableInput=table_input)
        return {'start': start_date}
    except Exception:
        return None


def handler(event, context):
    print(f"Event: {json.dumps(event)}")

    crawler_name = event['detail']['crawlerName']
    state = event['detail']['state']

    if state != 'Succeeded':
        return {'statusCode': 200}

    database_name = glue.get_crawler(Name=crawler_name)['Crawler']['DatabaseName']
    tables = [
        t for page in glue.get_paginator('get_tables').paginate(DatabaseName=database_name)
        for t in page.get('TableList', [])
    ]

    projections = {}
    for table in tables:
        dedupe_table_schema(database_name, table['Name'])
        result = add_partition_projection(database_name, table['Name'])
        if result:
            projections[table['Name']] = result

    message = (
        f"Crawler: {crawler_name}\n"
        f"Database: {database_name}\n"
        f"Tables: {len(tables)}\n"
        f"Projection enabled: {list(projections.keys())}"
    )
    sns.publish(
        TopicArn=os.environ['SNS_TOPIC_ARN'],
        Subject=f"Crawler Complete: {crawler_name}",
        Message=message
    )
    return {'statusCode': 200}
