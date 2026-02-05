# AWS Glue Crawler Design Document
## Hybrid Schema Discovery with Partition Projection

### Overview

This document describes the architecture of the hybrid blockchain schema discovery solution. The system uses Glue Crawlers for **one-time schema discovery**, then enables **partition projection** for automatic partition resolution. This eliminates ongoing crawler costs while maintaining automatic discovery of new blockchains.

---

## Design Goals

1. **Zero-Touch Discovery**: Automatically detect and catalog new blockchains
2. **Database Per Blockchain**: Each blockchain gets its own dedicated Glue database
3. **One-Time Crawling**: Crawlers run once to discover schema, then disabled
4. **Partition Projection**: Athena calculates partitions in-memory (no metadata lookups)
5. **Cost Optimization**: Near-zero ongoing costs after initial setup
6. **Simplicity**: Minimal Lambda functions, leverage AWS native features

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  AWS Public Blockchain S3                    │
│  v1.0/btc/  v1.0/eth/  v1.1/ton/  v1.0/newchain/           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              BlockchainDiscoveryFunction (Lambda)            │
│  1. Scans S3 for blockchain namespaces (weekly)             │
│  2. Creates Glue database per blockchain                    │
│  3. Creates Glue crawler (disabled by default)              │
│  4. Starts crawler ONCE on first creation                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│           AWS Glue Crawlers (one-time execution)             │
│  {stack}-BTC-Crawler → btc database (runs once)            │
│  {stack}-ETH-Crawler → eth database (runs once)            │
│  {stack}-TON-Crawler → ton database (runs once)            │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              CrawlerCompletionHandler (Lambda)               │
│  - Triggered when crawler completes                         │
│  - Adds partition projection properties to tables           │
│  - Sends SNS notification                                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                  AWS Glue Data Catalog                       │
│  Tables with partition projection enabled:                  │
│  - projection.enabled = true                                │
│  - projection.date.range = GENESIS,NOW                      │
│  - storage.location.template = s3://.../date=${date}        │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                     Amazon Athena                            │
│  - Calculates partitions in-memory (no Glue lookups)        │
│  - Automatically discovers new date partitions              │
│  - Faster query planning for highly partitioned tables      │
└─────────────────────────────────────────────────────────────┘
```

---

## Partition Projection

### What is Partition Projection?

Partition projection allows Athena to calculate partition values and locations in-memory rather than retrieving them from the Glue Data Catalog. For date-partitioned data like blockchain datasets, this provides:

- **Faster queries**: No metadata lookup latency
- **Automatic partition discovery**: New dates are calculated, not crawled
- **Zero ongoing cost**: No crawler runs needed for new partitions

### Configuration

After a crawler completes, the `CrawlerCompletionHandler` adds these properties to each table:

```sql
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.date.type' = 'date',
  'projection.date.range' = '2009-01-03,NOW',  -- Genesis to today
  'projection.date.format' = 'yyyy-MM-dd',
  'projection.date.interval' = '1',
  'projection.date.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://aws-public-blockchain/v1.0/btc/blocks/date=${date}'
)
```

### Known Genesis Dates

The system automatically detects the oldest date partition from S3 for each table. Since partitions are named `date=YYYY-MM-DD` and S3 lists objects lexicographically, the first partition returned is the oldest.

```python
def get_oldest_partition_date(s3_location):
    # List date= partitions (lexicographic order = chronological)
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix + '/date=',
        Delimiter='/',
        MaxKeys=1,
        RequestPayer='requester'
    )
    # Returns earliest date like '2009-01-03' for Bitcoin
```

If detection fails (e.g., empty table), falls back to `2020-01-01`.

---

## Component Details

### 1. BlockchainDiscoveryFunction

**Purpose**: Discovers blockchains and creates all necessary resources

**Trigger**: 
- EventBridge schedule (weekly by default)
- Manual invocation
- CloudFormation custom resource (on stack creation)

**Process**:
1. Scan S3 bucket for blockchain namespaces (v1.0/*, v1.1/*)
2. For each discovered blockchain:
   - Create Glue database if not exists
   - Create Glue crawler (disabled by default) if not exists
   - Start crawler ONCE on first creation
3. Send SNS notification with discovery report

**Environment Variables**:
- `S3_BUCKET`: Source bucket
- `SCHEMA_VERSION`: Default schema version
- `SCHEMA_VERSION_TON`: TON schema version
- `CRAWLER_ROLE_ARN`: IAM role for crawlers
- `STACK_NAME`: CloudFormation stack name
- `SNS_TOPIC_ARN`: Notification topic
- `DEFAULT_CRAWLER_SCHEDULE`: Default schedule (disabled by default)

**Schedule Mapping**:
```python
SCHEDULE_MAP = {
    'hourly': 'cron(0 * * * ? *)',
    'daily': 'cron(0 0 * * ? *)',
    'weekly': 'cron(0 0 ? * SUN *)',
    'disabled': None  # DEFAULT - partition projection handles new partitions
}
```

### 2. CrawlerCompletionHandler

**Purpose**: Add partition projection and send notifications when crawlers complete

**Trigger**: EventBridge rule on Glue Crawler State Change events

**Process**:
1. Receive crawler completion event
2. For each discovered table:
   - Remove duplicate columns (partition key collisions)
   - Add partition projection properties
3. Send SNS notification with discovered tables and projection status

### 3. Glue Crawlers

**Naming**: `{stack-name}-{BLOCKCHAIN}-Crawler`

**Configuration**:
- `Schedule`: Disabled by default (partition projection handles new partitions)
- `S3Targets.SampleSize`: 10 (samples only 10 files per table for schema inference)
- `RecrawlBehavior`: CRAWL_NEW_FOLDERS_ONLY (cost optimization)
- `SchemaChangePolicy.UpdateBehavior`: LOG (required for CRAWL_NEW_FOLDERS_ONLY)
- `SchemaChangePolicy.DeleteBehavior`: LOG (required for CRAWL_NEW_FOLDERS_ONLY)

**Note**: Crawlers are only needed for initial schema discovery. After partition projection is enabled, Athena automatically discovers new date partitions without crawling.

---

## Data Flow

### New Blockchain Discovery

```
1. New blockchain added to S3: s3://aws-public-blockchain/v1.0/sol/

2. Discovery Lambda runs (weekly or manual)
   - Scans S3, finds "sol" namespace
   
3. Creates resources:
   - Database: sol
   - Crawler: {stack}-SOL-Crawler (disabled)
   
4. Starts crawler ONCE immediately

5. Crawler infers schema from Parquet metadata
   - Creates tables: sol.blocks, sol.transactions, etc.
   
6. CrawlerCompletionHandler triggers:
   - Adds partition projection to all tables
   - Sends SNS notification
   
7. Data queryable in Athena with automatic partition discovery:
   SELECT * FROM sol.blocks WHERE date = '2024-01-15';
   -- No crawler needed for new dates!
```

### When to Re-run Crawlers

Crawlers only need to run again if:
- **Schema changes**: New columns added to blockchain data
- **New tables**: New table types added (e.g., new `events` table)

To manually trigger a crawler:
```bash
aws glue start-crawler --name {stack}-SOL-Crawler
```

To enable scheduled crawling (if needed):
```bash
aws glue update-crawler \
  --name {stack}-SOL-Crawler \
  --schedule "cron(0 0 ? * SUN *)"  # Weekly
```

---

## Design Decisions

### 1. Partition Projection by Default

**Decision**: Use partition projection instead of ongoing crawling

**Rationale**:
- Blockchain data uses predictable `date=YYYY-MM-DD` partitions
- Partition projection calculates partitions in-memory (faster queries)
- No ongoing crawler costs
- New partitions discovered automatically

### 2. One-Time Crawling

**Decision**: Crawlers run once for schema discovery, then disabled

**Rationale**:
- Schema rarely changes for blockchain data
- Partition projection handles new date partitions
- Significant cost savings (no daily/hourly crawler runs)
- Users can re-enable crawling if schema changes

### 3. Weekly Discovery Schedule

**Decision**: Keep weekly discovery for NEW blockchains

**Rationale**:
- AWS adds new blockchains periodically
- Discovery Lambda is lightweight (just S3 listing)
- Ensures new chains are automatically onboarded

### 4. CRAWL_NEW_FOLDERS_ONLY with LOG Policies

**Decision**: Use incremental crawling with LOG-only schema policies

**Rationale**:
- Cost optimization (only crawl new data)
- AWS requirement: CRAWL_NEW_FOLDERS_ONLY requires LOG for both UpdateBehavior and DeleteBehavior
- Schema changes are logged but don't modify existing tables

---

## Cost Analysis

### Hybrid Approach (Partition Projection)

| Component | Frequency | Est. Monthly Cost |
|-----------|-----------|-------------------|
| Discovery Lambda | Weekly | ~$0.01 |
| Initial Crawler (per chain) | Once | ~$0.02 |
| Ongoing Crawlers | None | $0 |
| **Total per chain** | | **~$0.02 one-time** |

### Comparison with Crawler-Only Approach

| Approach | Monthly Cost (10 chains) |
|----------|-------------------------|
| Daily crawlers | $5-20 |
| Weekly crawlers | $1.20 |
| **Hybrid (partition projection)** | **~$0.01** |

### When to Enable Scheduled Crawling

Only enable scheduled crawling if:
- Schema changes frequently
- New table types are added regularly
- You need to track schema evolution

For detailed cost analysis, see [COST_ANALYSIS.md](./COST_ANALYSIS.md).

---

## File Exclusions

Crawlers are configured to exclude non-Parquet files to prevent issues with blockchains that include additional data formats:

```python
'Exclusions': [
    '**/*.xdr',           # Stellar XDR files
    '**/*.xdr.zstd',      # Stellar compressed XDR files
    '**/*.json',          # JSON metadata
    '**/*.csv',           # CSV exports
    '**/*.txt',           # Text files
    '**/_SUCCESS',        # Spark success markers
    '**/_metadata',       # Parquet metadata
    '**/_common_metadata' # Parquet common metadata
]
```

---

## Stellar Blockchain Handling

Stellar has a unique nested folder structure that requires special handling:

```
v1.1/stellar/
├── ledgers/                          ← XDR files (.xdr.zstd) - EXCLUDED
│   ├── pubnet/
│   │   └── YYYY-MM-DD/               ← NOT Hive-style partitions
│   │       └── HASH--range/
│   │           └── *.xdr.zstd
│   └── testnet/
│       └── ...
└── parquet/                          ← Parquet files - CRAWLED
    ├── pubnet/
    │   └── v1/
    │       └── date=YYYY-MM-DD/      ← Hive-style partitions
    │           └── *.parquet
    └── testnet/
        └── v1/
            └── date=YYYY-MM-DD/
                └── *.parquet
```

### Discovery Logic

1. **Parquet folder detection**: When a `parquet/` folder exists, the discovery logic **only crawls that folder**, ignoring sibling directories like `ledgers/`. This prevents scanning millions of raw XDR files.

2. **Network folder detection**: Network folders (`pubnet`, `testnet`) create separate databases:
   - `stellar_pubnet` database
   - `stellar_testnet` database

3. **Version folder detection**: Version folders (`v1`, `v2`) containing Hive-style partitions (`date=YYYY-MM-DD`) are identified as table roots. The crawler path is set at the version folder level.

4. **Hive partition recognition**: The crawler configuration includes `TableGroupingPolicy: CombineCompatibleSchemas` to ensure all `date=` partitions are grouped into a single table rather than creating separate tables per partition.

### Crawler Configuration for Stellar

The `TableLevelConfiguration` is calculated dynamically based on the S3 path depth:

```python
# s3://bucket/a/b/c/ = 3 levels
path_parts = s3_path.replace('s3://', '').split('/')
table_level = len([p for p in path_parts if p])
```

| Chain | S3 Path | TableLevelConfiguration |
|-------|---------|------------------------|
| BTC/ETH | `s3://.../v1.0/btc/` | 3 |
| Stellar pubnet | `s3://.../v1.1/stellar/parquet/pubnet/v1/` | 6 |

Full crawler configuration:
```python
'Configuration': json.dumps({
    "Version": 1.0,
    "Grouping": {
        "TableGroupingPolicy": "CombineCompatibleSchemas",
        "TableLevelConfiguration": table_level  # Dynamic based on path
    },
    "CrawlerOutput": {
        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
        "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
    }
})
```

### Expected Results

| Database | Crawler Path | Table |
|----------|--------------|-------|
| `stellar_pubnet` | `s3://aws-public-blockchain/v1.1/stellar/parquet/pubnet/v1/` | `v1` (partitioned by `date`) |
| `stellar_testnet` | `s3://aws-public-blockchain/v1.1/stellar/parquet/testnet/v1/` | `v1` (partitioned by `date`) |

---

## Limitations

1. **Minimum schedule**: 1 minute (Glue cron limit)
2. **Parquet only**: Assumes data is in Parquet format (non-Parquet files are excluded)
3. **S3 structure**: Assumes `{version}/{blockchain}/` structure
4. **Concurrent crawlers**: AWS Glue has soft limits on concurrent crawlers
5. **Schema changes**: With CRAWL_NEW_FOLDERS_ONLY, schema changes are logged but not applied
6. **Table limit**: Glue has a 200,000 table limit per database

---

## Monitoring

### CloudWatch Metrics

- Crawler: `glue.driver.aggregate.numBytes`, `elapsedTime`
- Lambda: `Invocations`, `Errors`, `Duration`

### Logs

- `/aws-glue/crawlers` - Crawler execution
- `/aws/lambda/{stack}-BlockchainDiscovery` - Discovery
- `/aws/lambda/{stack}-CrawlerCompletionHandler` - Completions

### Alerts

Subscribe to SNS topic for:
- New blockchain discoveries
- Crawler completions
- Error notifications

---

## Security

### IAM Roles

| Role | Purpose | Key Permissions |
|------|---------|-----------------|
| GlueCrawlerRole | Crawler execution | S3 read, Glue catalog |
| BlockchainDiscoveryRole | Discovery Lambda | S3 list, Glue create/start |
| CrawlerCompletionHandlerRole | Completion Lambda | Glue read/update, SNS publish |

### Resource Scoping

All crawlers are scoped to `{stack-name}-*` pattern.

---

## Extensibility

### Adding New Genesis Dates

Genesis dates are now auto-detected from S3 by listing the oldest `date=` partition. No manual configuration needed for new blockchains.

### Custom Processing

Extend `CrawlerCompletionHandler` to:
- Trigger data pipelines
- Update dashboards
- Send Slack notifications
- Create Athena views
