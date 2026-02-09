"""
Blockchain Discovery Lambda
Discovers blockchain namespaces in S3 and creates Glue databases + crawlers.
Uses manifest.json as primary source for chain names and paths,
with heuristic S3 scanning as fallback for chains not in the manifest.
"""
import boto3
import json
import os
import urllib.request

glue = boto3.client('glue')
s3 = boto3.client('s3')
sns = boto3.client('sns')

MANIFEST_URL = 'https://aws-public-blockchain.s3.us-east-2.amazonaws.com/manifest.json'

SCHEDULE_MAP = {
    'hourly': 'cron(0 * * * ? *)',
    'daily': 'cron(0 0 * * ? *)',
    'weekly': 'cron(0 0 ? * SUN *)',
    'disabled': None
}

# Folders that indicate blockchain table data
TABLE_INDICATORS = {
    'blocks', 'transactions', 'logs', 'traces', 'token_transfers',
    'contracts', 'messages'
}

# Network variants (e.g., Stellar has pubnet/testnet)
NETWORK_INDICATORS = {'pubnet', 'testnet', 'mainnet'}

# Version folders containing date partitions
VERSION_FOLDERS = {'v1', 'v2', 'v3'}


def has_hive_partitions(bucket, prefix):
    """Check if folder contains Hive-style partitions (key=value format)."""
    try:
        response = s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter='/',
            MaxKeys=10, RequestPayer='requester'
        )
        return any(
            '=' in p['Prefix'].split('/')[-2]
            for p in response.get('CommonPrefixes', [])
        )
    except Exception:
        return False


def is_blockchain_folder(bucket, prefix):
    """
    Determine if a folder contains blockchain data.
    Returns False if 'parquet' folder exists (needs deeper scanning).
    """
    try:
        response = s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter='/',
            MaxKeys=20, RequestPayer='requester'
        )
        folders = [
            p['Prefix'].rstrip('/').split('/')[-1].lower()
            for p in response.get('CommonPrefixes', [])
        ]
        if 'parquet' in folders:
            return False
        return any(
            f in TABLE_INDICATORS or f in VERSION_FOLDERS or f.startswith('date=')
            for f in folders
        )
    except Exception:
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
        all_prefixes = [
            (p['Prefix'], p['Prefix'].rstrip('/').split('/')[-1].lower())
            for page in paginator.paginate(
                Bucket=bucket, Prefix=prefix, Delimiter='/',
                RequestPayer='requester'
            )
            for p in page.get('CommonPrefixes', [])
        ]

        # If parquet folder exists, only scan that (skip siblings like 'ledgers')
        parquet_prefix = next((p for p, f in all_prefixes if f == 'parquet'), None)
        if parquet_prefix:
            scan_for_blockchains(bucket, parquet_prefix, base_name, discovered, paginator, depth + 1)
            return

        for sub_prefix, folder in all_prefixes:
            if folder in VERSION_FOLDERS:
                if has_hive_partitions(bucket, sub_prefix) and base_name not in discovered:
                    discovered[base_name] = f"s3://{bucket}/{sub_prefix}"
                else:
                    scan_for_blockchains(bucket, sub_prefix, base_name, discovered, paginator, depth + 1)
            elif '=' in folder:
                continue
            elif is_blockchain_folder(bucket, sub_prefix):
                if folder in NETWORK_INDICATORS:
                    scan_for_blockchains(bucket, sub_prefix, f"{base_name}_{folder}", discovered, paginator, depth + 1)
                elif folder in TABLE_INDICATORS:
                    if base_name not in discovered:
                        discovered[base_name] = f"s3://{bucket}/{prefix}"
                    return
                else:
                    sub_name = f"{base_name}_{folder}"
                    if sub_name not in discovered:
                        discovered[sub_name] = f"s3://{bucket}/{sub_prefix}"
            elif folder in NETWORK_INDICATORS:
                scan_for_blockchains(bucket, sub_prefix, f"{base_name}_{folder}", discovered, paginator, depth + 1)
    except Exception:
        pass


def discover_blockchains(bucket, version):
    """Discover blockchain namespaces in a schema version folder via S3 heuristic."""
    discovered = {}
    paginator = s3.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(
            Bucket=bucket, Prefix=f"{version}/", Delimiter='/',
            RequestPayer='requester'
        ):
            for p in page.get('CommonPrefixes', []):
                prefix = p['Prefix']
                name = prefix.strip('/').split('/')[1].lower()
                if is_blockchain_folder(bucket, prefix):
                    discovered[name] = f"s3://{bucket}/{prefix}"
                else:
                    scan_for_blockchains(bucket, prefix, name, discovered, paginator)
    except Exception:
        pass
    return discovered


def fetch_manifest(bucket):
    """Fetch manifest.json for authoritative chain names and paths."""
    try:
        with urllib.request.urlopen(MANIFEST_URL, timeout=10) as resp:
            manifest = json.loads(resp.read().decode())
        return {
            chain['name']: f"s3://{bucket}/{chain['path'].strip('/')}/"
            for chain in manifest.get('chains', [])
        }
    except Exception:
        return None


def create_crawler(name, role, db_name, s3_path, schedule):
    """Create a Glue crawler for a blockchain."""
    depth = len([p for p in s3_path.replace('s3://', '').split('/') if p])

    config = {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
            "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
        }
    }
    if depth > 4:
        config["Grouping"] = {
            "TableGroupingPolicy": "CombineCompatibleSchemas",
            "TableLevelConfiguration": depth
        }

    params = {
        'Name': name,
        'Role': role,
        'DatabaseName': db_name,
        'Targets': {
            'S3Targets': [{
                'Path': s3_path,
                'SampleSize': 10,
                'Exclusions': [
                    '**/*.xdr*', '**/*.json', '**/*.csv', '**/_*'
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
    schedule_expr = SCHEDULE_MAP.get(default_schedule)

    # Primary: manifest for authoritative names and paths
    chains = fetch_manifest(bucket) or {}
    manifest_names = set(chains.keys())

    # Heuristic fallback for chains not in manifest
    heuristic = {}
    for version in [schema_v1, schema_v2]:
        for name, path in discover_blockchains(bucket, version).items():
            if name not in heuristic:
                heuristic[name] = path

    # Clean up old heuristic-named resources replaced by manifest names
    # e.g., "btc" database -> "aws_bitcoin_mainnet" from manifest
    manifest_paths = {p.rstrip('/'): name for name, p in chains.items()}
    for old_name, path in heuristic.items():
        manifest_name = manifest_paths.get(path.rstrip('/'))
        if manifest_name and old_name != manifest_name:
            old_crawler = f"{stack_name}-{old_name.upper()}-Crawler"
            try:
                glue.get_crawler(Name=old_crawler)
                glue.delete_crawler(Name=old_crawler)
                print(f"Deleted old crawler: {old_crawler}")
            except Exception:
                pass
            try:
                glue.get_database(Name=old_name.lower())
                glue.delete_database(Name=old_name.lower())
                print(f"Deleted old db: {old_name.lower()}")
            except Exception:
                pass
        elif path.rstrip('/') not in manifest_paths:
            # Chain not in manifest at all — add with heuristic name
            chains[old_name] = path

    print(f"Discovered {len(chains)} blockchains ({len(manifest_names)} from manifest): {list(chains.keys())}")

    created_dbs = []
    created_crawlers = []
    started_crawlers = []

    for chain, s3_path in chains.items():
        db_name = chain.lower()
        crawler_name = f"{stack_name}-{chain.upper()}-Crawler"

        # Create database if needed
        try:
            glue.get_database(Name=db_name)
        except Exception:
            glue.create_database(DatabaseInput={
                'Name': db_name,
                'Description': f'[{stack_name}] {chain.upper()}'
            })
            created_dbs.append(db_name)
            print(f"Created database: {db_name}")

        # Create crawler if needed
        try:
            glue.get_crawler(Name=crawler_name)
        except Exception:
            create_crawler(crawler_name, crawler_role, db_name, s3_path, schedule_expr)
            created_crawlers.append(crawler_name)
            print(f"Created crawler: {crawler_name}")

            # Start crawler immediately on first creation
            if glue.get_crawler(Name=crawler_name)['Crawler']['State'] == 'READY':
                glue.start_crawler(Name=crawler_name)
                started_crawlers.append(crawler_name)
                print(f"Started crawler: {crawler_name}")

    if created_dbs or created_crawlers:
        sns.publish(
            TopicArn=sns_topic,
            Subject=f"Discovery: {len(chains)} blockchains found",
            Message=(
                f"DBs: {created_dbs}\n"
                f"Crawlers: {created_crawlers}\n"
                f"Started: {started_crawlers}"
            )
        )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'chains': list(chains.keys()),
            'dbs': created_dbs,
            'crawlers': created_crawlers
        })
    }
