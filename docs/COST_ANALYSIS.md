# Cost Analysis: Blockchain Crawler Architecture

This document provides a comprehensive cost breakdown for the AWS Public Blockchain crawler architecture, along with optimization strategies for different use cases.

---

## Architecture Overview

The crawler architecture consists of:
- **Lambda Functions**: Discovery and completion handling
- **Glue Crawlers**: Schema discovery per blockchain
- **S3**: Athena query results storage
- **SNS**: Notifications
- **EventBridge**: Scheduling and event routing
- **Athena**: Query execution

---

## Sample Size Optimization

Crawlers are configured with `SampleSize: 10` on S3Targets, which limits schema inference to 10 files per table. This dramatically reduces crawl time and cost for large datasets while still discovering all partitions.

### Impact: Stellar Blockchain Example

Stellar contains **60+ million Parquet files** across its tables. Without sampling:

| Metric | Without Sampling | With Sampling (10 files) |
|--------|------------------|--------------------------|
| Files scanned for schema | 60,000,000+ | ~100 (10 per table) |
| Crawl duration | 4-8+ hours | 2-5 minutes |
| S3 LIST API calls | Millions | Thousands |
| Estimated crawl cost | $5-15 | $0.05-0.10 |

### Why This Works

1. **Parquet stores schema in file metadata** - reading 10 files gives the same schema as reading 60 million
2. **Partition discovery is separate** - the crawler still walks the directory structure to find all `date=YYYY-MM-DD/` partitions
3. **`CRAWL_NEW_FOLDERS_ONLY`** - subsequent crawls only look at new partition folders, not existing files

### Configuration

```python
'S3Targets': [{
    'Path': s3_path,
    'SampleSize': 10,
    'Exclusions': ['**/*.xdr', ...]
}]
```

### What's NOT Affected

- ✅ All partitions are discovered and cataloged
- ✅ All data is queryable in Athena
- ✅ New daily partitions are detected on each crawl

### Edge Case: Schema Evolution

If AWS adds new columns to the Parquet files in the future, the sampled files might not include them. Mitigation options:
- Run a one-time full crawl (remove sample size limit temporarily)
- Manually add columns via `ALTER TABLE ADD COLUMNS`

This is rare for blockchain data since schemas are stable.

---

## Default Cost Model (Optimized for Completeness)

The default configuration prioritizes **data completeness** and **rapid discovery of new chains** while minimizing unnecessary crawler runs.

### Key Design Decisions

| Component | Default Setting | Rationale |
|-----------|-----------------|-----------|
| Discovery Schedule | Weekly (Sunday 2AM) | New blockchains are added infrequently |
| Crawler Schedule | Daily | Catches new partitions within 24 hours |
| Recrawl Behavior | `CRAWL_NEW_FOLDERS_ONLY` | Only scans new data, reduces cost |
| Schema Policy | `LOG` | Tracks changes without modifying tables |

### Monthly Cost Breakdown (Default Configuration)

#### Fixed Costs

| Service | Resource | Monthly Cost |
|---------|----------|--------------|
| S3 | Athena Results Bucket | $0.50-2.00 |
| SNS | Notification Topic | $0.50 |
| SSM | Parameters (3) | Free |
| IAM | Roles | Free |
| EventBridge | Rules (2) | Free |

**Fixed Total: ~$1-3/month**

#### Lambda Costs

| Function | Invocations | Duration | Monthly Cost |
|----------|-------------|----------|--------------|
| BlockchainDiscovery | 4-5/month | 60s @ 256MB | $0.01 |
| CrawlerCompletionHandler | Per crawler run | 5s @ 128MB | $0.01-0.10 |
| InitialDiscoveryTrigger | Deploy only | One-time | Negligible |

**Lambda Total: ~$0.05-0.15/month**

#### Glue Crawler Costs (Per Chain)

With `TableLevelSampleSize: 10`, crawl times are dramatically reduced. Costs below reflect optimized crawls:

| Schedule | Runs/Month | Cost/Chain* | 5 Chains | 10 Chains |
|----------|------------|-------------|----------|-----------|
| **daily** | 30 | $0.15-0.50 | $0.75-2.50 | $1.50-5.00 |
| hourly | 720 | $0.50-1.00 | $2.50-5.00 | $5.00-10.00 |
| weekly | 4 | $0.04-0.12 | $0.20-0.60 | $0.40-1.20 |

*Lower end assumes incremental crawls (`CRAWL_NEW_FOLDERS_ONLY`), higher end for initial/full crawls.

> **Glue Pricing**: ~$0.44/DPU-hour. With sampling enabled, even large chains like Stellar complete in minutes instead of hours.

#### Athena Query Costs

| Usage Level | Queries/Day | Data Scanned/Month | Monthly Cost |
|-------------|-------------|-------------------|--------------|
| Light | 10 | ~10GB | $0.50 |
| Moderate | 50 | ~100GB | $5.00 |
| Heavy | 200+ | ~500GB | $25.00 |

> **Athena Pricing**: $5 per TB scanned

### Total Monthly Estimates (Default)

| Scenario | Chains | Athena Usage | **Total** |
|----------|--------|--------------|-----------|
| Minimal | 3 | Light | **$2-4** |
| Typical | 5 | Moderate | **$6-10** |
| Production | 10 | Heavy | **$30-40** |

> Note: Costs reduced from previous estimates due to `TableLevelSampleSize` optimization.

---

## Cost Optimization Strategies

### Strategy 1: Reduce Crawler Frequency (Recommended)

Since blockchain data schemas rarely change, crawlers primarily detect new partitions. For most use cases, **weekly or monthly** crawls are sufficient.

```bash
# Change to weekly schedule
aws glue update-crawler \
  --name blockchain-crawlers-BTC-Crawler \
  --schedule "cron(0 0 ? * SUN *)"

# Change to monthly schedule (1st of month)
aws glue update-crawler \
  --name blockchain-crawlers-ETH-Crawler \
  --schedule "cron(0 0 1 * ? *)"
```

**Savings**: 85-95% reduction in crawler costs

| Schedule | Cost/Chain/Month |
|----------|------------------|
| Daily | $0.50 |
| Weekly | $0.12 |
| Monthly | $0.03 |

### Strategy 2: Disable Scheduled Crawling

For maximum cost control, disable automatic crawling and trigger manually when needed.

**Deploy with crawling disabled:**
```bash
aws cloudformation create-stack \
  --stack-name blockchain-crawlers \
  --template-body file://data-consumer/aws-public-blockchain-with-crawlers.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters ParameterKey=DefaultCrawlerSchedule,ParameterValue=disabled
```

**Disable existing crawler:**
```bash
aws glue update-crawler \
  --name blockchain-crawlers-BTC-Crawler \
  --schedule ""
```

**Trigger manually when needed:**
```bash
aws glue start-crawler --name blockchain-crawlers-BTC-Crawler
```

**When to trigger manually:**
- AWS announces a schema update
- New blockchain added to the bucket
- Missing columns in query results
- After initial deployment

**Savings**: 95%+ reduction in crawler costs (pay only when you run)

### Strategy 3: Use Partition Projection (Athena)

Instead of relying on crawlers to discover new partitions, use Athena partition projection for date-partitioned data.

```sql
-- Example: Create table with partition projection
CREATE EXTERNAL TABLE btc.blocks_projected (
  -- columns here
)
PARTITIONED BY (date string)
LOCATION 's3://aws-public-blockchain/v1.0/btc/blocks/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.date.type' = 'date',
  'projection.date.range' = '2009-01-03,NOW',
  'projection.date.format' = 'yyyy-MM-dd',
  'storage.location.template' = 's3://aws-public-blockchain/v1.0/btc/blocks/date=${date}'
);
```

**Benefits:**
- No crawler needed for partition discovery
- Queries work immediately on new dates
- Zero additional cost

**Limitations:**
- Requires knowing the schema upfront
- Won't detect schema changes automatically

### Strategy 4: Optimize Athena Queries

Reduce Athena costs by scanning less data:

```sql
-- BAD: Full table scan
SELECT * FROM btc.blocks LIMIT 100;

-- GOOD: Partition filter
SELECT * FROM btc.blocks WHERE date = '2024-01-15' LIMIT 100;

-- GOOD: Date range filter
SELECT * FROM btc.blocks 
WHERE date BETWEEN '2024-01-01' AND '2024-01-31';

-- GOOD: Select only needed columns
SELECT block_number, block_hash, transaction_count 
FROM btc.blocks 
WHERE date = '2024-01-15';
```

**Savings**: 50-90% reduction in Athena costs with proper filtering

### Strategy 5: Selective Chain Monitoring

Not all chains need the same monitoring frequency. Tier your chains:

| Tier | Chains | Schedule | Use Case |
|------|--------|----------|----------|
| Active | BTC, ETH | Daily | Frequently queried |
| Standard | Others | Weekly | Occasional queries |
| Archive | Inactive | Disabled | Query on-demand |

```bash
# Set active chains to daily
aws glue update-crawler --name blockchain-crawlers-BTC-Crawler \
  --schedule "cron(0 0 * * ? *)"

# Set standard chains to weekly  
aws glue update-crawler --name blockchain-crawlers-TON-Crawler \
  --schedule "cron(0 0 ? * SUN *)"

# Disable archive chains
aws glue update-crawler --name blockchain-crawlers-DOGE-Crawler \
  --schedule ""
```

---

## Cost Scenarios

### Scenario A: Development/Testing
- 3 chains, weekly crawls, light queries
- **Monthly cost: $1.50-3**

### Scenario B: Production (Cost-Optimized)
- 5 chains, weekly crawls, moderate queries
- **Monthly cost: $5-8**

### Scenario C: Production (Default)
- 5 chains, daily crawls, moderate queries
- **Monthly cost: $6-10**

### Scenario D: High-Frequency Monitoring
- 5 chains, hourly crawls, heavy queries
- **Monthly cost: $25-40**

### Scenario E: Enterprise (Many Chains)
- 15 chains, daily crawls, heavy queries
- **Monthly cost: $40-60**

> All scenarios assume `TableLevelSampleSize: 10`. Without sampling, costs for large chains like Stellar would be 10-50x higher.

---

## Monitoring Costs

### Set Up Billing Alerts

```bash
# Create billing alarm at $10
aws cloudwatch put-metric-alarm \
  --alarm-name "BlockchainCrawler-Cost-10" \
  --alarm-description "Alert when costs exceed $10" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 86400 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=Currency,Value=USD \
  --evaluation-periods 1 \
  --alarm-actions <your-sns-topic-arn>
```

### Track Crawler Costs

```bash
# View crawler run history
aws glue get-crawler-metrics \
  --crawler-name-list blockchain-crawlers-BTC-Crawler

# Check CloudWatch for crawler duration
aws logs filter-log-events \
  --log-group-name /aws-glue/crawlers \
  --filter-pattern "blockchain-crawlers"
```

---

## Quick Reference

### Recommended Settings by Use Case

| Use Case | Discovery | Crawler Schedule | Est. Cost (5 chains) |
|----------|-----------|------------------|----------------------|
| Cost-Sensitive | Weekly | Weekly | $2-4/month |
| Balanced | Weekly | Daily | $6-10/month |
| Data-Critical | Daily | Daily | $8-12/month |
| High-Frequency | Daily | Hourly | $25-40/month |

### Commands Cheat Sheet

```bash
# List all crawlers
aws glue list-crawlers --query 'CrawlerNames[?starts_with(@, `blockchain-crawlers-`)]'

# Check crawler schedule
aws glue get-crawler --name <crawler-name> --query 'Crawler.Schedule'

# Update schedule
aws glue update-crawler --name <crawler-name> --schedule "cron(0 0 ? * SUN *)"

# Disable schedule
aws glue update-crawler --name <crawler-name> --schedule ""

# Manual trigger
aws glue start-crawler --name <crawler-name>

# Check Athena query costs
aws athena list-query-executions --work-group AWSPublicBlockchain
```

---

## Summary

The default architecture costs **$6-10/month** for typical usage with 5 chains and daily crawls. The `TableLevelSampleSize: 10` setting ensures even large chains like Stellar (60M+ files) crawl in minutes instead of hours.

For cost-sensitive deployments:

1. Switch to **weekly crawls** → saves ~85%
2. Use **partition projection** → eliminates partition discovery crawls
3. **Filter queries by date** → reduces Athena costs 50-90%
4. **Disable unused chains** → pay only for what you use

Most users can run this architecture for **$2-4/month** with weekly crawls and basic query optimization.
