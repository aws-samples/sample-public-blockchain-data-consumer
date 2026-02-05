"""
Blockchain Discovery Lambda
Discovers blockchain namespaces in S3 and creates Glue databases + crawlers.
"""
import boto3
import json
import os

glue = boto3.client('glue')
s3 = boto3.client('s3')
sns = boto3.client('sns')

SCHEDULE_MAP = {
    'hourly': 'cron(0 * * * ? *)',
    'daily': 'cron(0 0 * * ? *)',
    'weekly': 'cron(0 0 ? * SUN *)',
    'disabled': None
}

# Folders that indicate blockchain table data
TABLE_INDICATORS = {
    'blocks', 'transactions', 'logs', 'traces', 'token_transfers',
    'contracts', 'receipts', 'events', 'transfers', 'balances',
    'accounts', 'messages', 'operations', 'ledgers', 'payments'
}

# Network variants (e.g., Stellar has pubnet/testnet)
NETWORK_INDICATORS = {'pubnet', 'testnet', 'mainnet', 'devnet'}

# Version folders containing date partitions
VERSION_FOLDERS = {'v1', 'v2', 'v3', 'v4', 'v5'}


def has_hive_partitions(bucket, prefix):
    """Check if folder contains Hive-style partitions (key=value format)."""
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/',
            MaxKeys=10,
            RequestPayer='requester'
        )
        for p in response.get('CommonPrefixes', []):
            folder = p['Prefix'].rstrip('/').split('/')[-1]
            if '=' in folder:
                return True
        return False
    except Exception as e:
        print(f"Error checking partitions in {prefix}: {e}")
        return False


def is_blockchain_folder(bucket, prefix):
    """
    Determine if a folder contains blockchain data.
    Returns False if 'parquet' folder exists (needs deeper scanning).
    """
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/',
            MaxKeys=20,
            RequestPayer='requester'
        )
        
        # Check for parquet files directly
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                return True
        
        # Get subfolder names
        folders = []
        for p in response.get('CommonPrefixes', []):
            folder = p['Prefix'].rstrip('/').split('/')[-1].lower()
            folders.append(folder)
            # If parquet folder exists, needs deeper scanning
            if folder == 'parquet':
                return False
        
        # Check for blockchain indicators
        for folder in folders:
            if folder in TABLE_INDICATORS:
                return True
            if folder in VERSION_FOLDERS:
                return True
            if folder.startswith('date='):
                return True
        
        return False
    except Exception as e:
        print(f"Error checking folder {prefix}: {e}")
        return False


def scan_for_blockchains(bucket, prefix, base_name, discovered, paginator, depth=0):
    """
    Recursively scan for blockchain data, handling nested structures like:
    - v1.1/stellar/parquet/pubnet/v1/date=.../
    - v1.1/sonarx/arbitrum/blocks/date=.../
    """
    if depth > 5:
        return
    
    try:
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/',
            RequestPayer='requester'
        )
        
        # Collect prefixes and check for parquet folder
        all_prefixes = []
        parquet_prefix = None
        
        for page in pages:
            for p in page.get('CommonPrefixes', []):
                sub_prefix = p['Prefix']
                folder = sub_prefix.rstrip('/').split('/')[-1].lower()
                all_prefixes.append((sub_prefix, folder))
                if folder == 'parquet':
                    parquet_prefix = sub_prefix
        
        # If parquet folder exists, only scan that (skip siblings like 'ledgers')
        if parquet_prefix:
            print(f"  Found parquet folder, scanning: {parquet_prefix}")
            scan_for_blockchains(bucket, parquet_prefix, base_name, discovered, paginator, depth + 1)
            return
        
        # Process subfolders
        for sub_prefix, folder in all_prefixes:
            # Version folders (v1, v2) - check for Hive partitions
            if folder in VERSION_FOLDERS:
                if has_hive_partitions(bucket, sub_prefix):
                    if base_name not in discovered:
                        discovered[base_name] = f"s3://{bucket}/{sub_prefix}"
                        print(f"Found: {base_name} at s3://{bucket}/{sub_prefix}")
                else:
                    scan_for_blockchains(bucket, sub_prefix, base_name, discovered, paginator, depth + 1)
                continue
            
            # Skip Hive partition folders
            if '=' in folder:
                continue
            
            # Check if folder contains blockchain tables
            if is_blockchain_folder(bucket, sub_prefix):
                if folder in NETWORK_INDICATORS:
                    # Network-specific database (stellar_pubnet)
                    scan_for_blockchains(bucket, sub_prefix, f"{base_name}_{folder}", discovered, paginator, depth + 1)
                elif folder in TABLE_INDICATORS:
                    # Found table folder - parent is blockchain root
                    if base_name not in discovered:
                        discovered[base_name] = f"s3://{bucket}/{prefix}"
                        print(f"Found: {base_name} at s3://{bucket}/{prefix}")
                    return
                else:
                    # Sub-chain under vendor namespace (sonarx/arbitrum)
                    sub_name = f"{base_name}_{folder}"
                    if sub_name not in discovered:
                        discovered[sub_name] = f"s3://{bucket}/{sub_prefix}"
                        print(f"Found: {sub_name} at s3://{bucket}/{sub_prefix}")
                continue
            
            # Network indicator folder
            if folder in NETWORK_INDICATORS:
                scan_for_blockchains(bucket, sub_prefix, f"{base_name}_{folder}", discovered, paginator, depth + 1)
                
    except Exception as e:
        print(f"Error scanning {prefix}: {e}")


def discover_blockchains(bucket, version):
    """Discover blockchain namespaces in a schema version folder."""
    discovered = {}
    paginator = s3.get_paginator('list_objects_v2')
    
    try:
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=f"{version}/",
            Delimiter='/',
            RequestPayer='requester'
        )
        
        for page in pages:
            for p in page.get('CommonPrefixes', []):
                prefix = p['Prefix']
                name = prefix.strip('/').split('/')[1].lower()
                
                if is_blockchain_folder(bucket, prefix):
                    discovered[name] = f"s3://{bucket}/{prefix}"
                    print(f"Found: {name} at s3://{bucket}/{prefix}")
                else:
                    print(f"Scanning {name} for nested data...")
                    scan_for_blockchains(bucket, prefix, name, discovered, paginator)
                    
    except Exception as e:
        print(f"Error discovering in {version}: {e}")
    
    return discovered


def create_crawler(name, role, db_name, s3_path, schedule):
    """Create a Glue crawler for a blockchain."""
    # Calculate path depth for table grouping
    depth = len([p for p in s3_path.replace('s3://', '').split('/') if p])
    
    config = {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
            "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
        }
    }
    
    # Deep paths need table grouping (Stellar-style)
    if depth > 4:
        config["Grouping"] = {
            "TableGroupingPolicy": "CombineCompatibleSchemas",
            "TableLevelConfiguration": depth
        }
    
    params = {
        'Name': name,
        'Role': role,
        'DatabaseName': db_name,
        'Description': f'Crawler for {db_name.upper()} blockchain',
        'Targets': {
            'S3Targets': [{
                'Path': s3_path,
                'SampleSize': 10,
                'Exclusions': [
                    '**/*.xdr', '**/*.xdr.zstd',
                    '**/*.json', '**/*.csv', '**/*.txt',
                    '**/_SUCCESS', '**/_metadata', '**/_common_metadata'
                ]
            }]
        },
        'SchemaChangePolicy': {
            'UpdateBehavior': 'LOG',
            'DeleteBehavior': 'LOG'
        },
        'RecrawlPolicy': {
            'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
        },
        'Configuration': json.dumps(config)
    }
    
    # Add schedule if not disabled
    if schedule:
        params['Schedule'] = schedule
    
    glue.create_crawler(**params)


def handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    bucket = os.environ['S3_BUCKET']
    schema_v1 = os.environ['SCHEMA_VERSION']
    schema_v2 = os.environ.get('SCHEMA_VERSION_TON', 'v1.1')
    crawler_role = os.environ['CRAWLER_ROLE_ARN']
    stack_name = os.environ['STACK_NAME']
    sns_topic = os.environ['SNS_TOPIC_ARN']
    default_schedule = os.environ.get('DEFAULT_CRAWLER_SCHEDULE', 'disabled')
    
    # Discover blockchains in both schema versions
    all_chains = {}
    for version in [schema_v1, schema_v2]:
        for name, path in discover_blockchains(bucket, version).items():
            if name not in all_chains:
                all_chains[name] = path
    
    print(f"Discovered {len(all_chains)} blockchains: {list(all_chains.keys())}")
    
    created_dbs = []
    created_crawlers = []
    started_crawlers = []
    schedule_expr = SCHEDULE_MAP.get(default_schedule)
    
    for chain, s3_path in all_chains.items():
        db_name = chain.lower()
        crawler_name = f"{stack_name}-{chain.upper()}-Crawler"
        
        # Create database if needed
        try:
            glue.get_database(Name=db_name)
        except glue.exceptions.EntityNotFoundException:
            glue.create_database(DatabaseInput={
                'Name': db_name,
                'Description': f'[{stack_name}] {chain.upper()} blockchain data'
            })
            created_dbs.append(db_name)
            print(f"Created database: {db_name}")
        
        # Create crawler if needed
        try:
            glue.get_crawler(Name=crawler_name)
        except glue.exceptions.EntityNotFoundException:
            create_crawler(crawler_name, crawler_role, db_name, s3_path, schedule_expr)
            created_crawlers.append(crawler_name)
            print(f"Created crawler: {crawler_name}")
            
            # Start crawler immediately
            if glue.get_crawler(Name=crawler_name)['Crawler']['State'] == 'READY':
                glue.start_crawler(Name=crawler_name)
                started_crawlers.append(crawler_name)
                print(f"Started crawler: {crawler_name}")
    
    # Send notification
    if created_dbs or created_crawlers:
        message = f"""Blockchain Discovery Report
============================
Chains found: {len(all_chains)}
Databases created: {created_dbs}
Crawlers created: {created_crawlers}
Crawlers started: {started_crawlers}
"""
        sns.publish(
            TopicArn=sns_topic,
            Subject=f"Discovery: {len(all_chains)} blockchains found",
            Message=message
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'discovered': list(all_chains.keys()),
            'databases': created_dbs,
            'crawlers': created_crawlers,
            'started': started_crawlers
        })
    }
