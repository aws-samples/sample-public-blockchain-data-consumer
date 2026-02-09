# Blockchain Data Consumer Architecture

CloudFormation templates for deploying the AWS infrastructure to consume and query blockchain data from the [AWS Public Blockchain Datasets](https://registry.opendata.aws/aws-public-blockchain/).

## Templates

| Template | Description |
|----------|-------------|
| `aws-public-blockchain-with-crawlers.yaml` | **Recommended** - Automatic schema discovery with Glue Crawlers |
| `aws-public-blockchain.yaml` | Original template with pre-defined schemas |

## Quick Start

### Deploy with Automatic Schema Discovery (Recommended)

```bash
# Package Lambda code and upload to S3
aws cloudformation package \
  --template-file data-consumer/aws-public-blockchain-with-crawlers.yaml \
  --s3-bucket YOUR_DEPLOYMENT_BUCKET \
  --output-template-file packaged.yaml

# Deploy the packaged template
aws cloudformation deploy \
  --template-file packaged.yaml \
  --stack-name blockchain-crawlers \
  --capabilities CAPABILITY_NAMED_IAM
```

The stack automatically:
- Fetches the [manifest.json](https://aws-public-blockchain.s3.us-east-2.amazonaws.com/manifest.json) for authoritative chain names
- Creates a dedicated database per blockchain using manifest names (e.g., `aws_bitcoin_mainnet`)
- Falls back to S3 heuristic scanning for chains not in the manifest
- Runs crawlers once to infer schemas, then enables partition projection
- No ongoing crawler costs — Athena calculates partitions in-memory

### Subscribe to Notifications

```bash
TOPIC_ARN=$(aws cloudformation describe-stacks \
  --stack-name blockchain-crawlers \
  --query 'Stacks[0].Outputs[?OutputKey==`CrawlerNotificationTopicArn`].OutputValue' \
  --output text)

aws sns subscribe \
  --topic-arn $TOPIC_ARN \
  --protocol email \
  --notification-endpoint your-email@example.com
```

### Query Data in Athena

```sql
SHOW DATABASES;
SHOW TABLES IN aws_bitcoin_mainnet;
SELECT * FROM aws_bitcoin_mainnet.blocks WHERE date = '2024-01-01' LIMIT 10;
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  AWS Public Blockchain S3                    │
│              + manifest.json (chain names & paths)          │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              BlockchainDiscoveryFunction (Lambda)            │
│  1. Fetches manifest.json for authoritative chain names     │
│  2. Falls back to S3 heuristic for unlisted chains          │
│  3. Creates database per chain (manifest naming)            │
│  4. Creates crawler per chain, starts it ONCE               │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│           AWS Glue Crawlers (one-time execution)             │
│  Infers schema from Parquet metadata, then disabled         │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              CrawlerCompletionHandler (Lambda)               │
│  Adds partition projection → no more crawling needed        │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                  AWS Glue Data Catalog                       │
│  aws_bitcoin_mainnet | aws_ethereum_mainnet | ton_mainnet   │
│  stellar_pubnet | sonarx_arbitrum_mainnet | etc.            │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                     Amazon Athena                            │
│  Partition projection: calculates partitions in-memory      │
│  New date partitions discovered automatically               │
└─────────────────────────────────────────────────────────────┘
```

---

## Managing Crawlers

Crawlers run once on initial discovery. Partition projection handles new date partitions automatically. Re-run crawlers only if schema changes.

### Manually Trigger a Crawler

```bash
aws glue start-crawler --name blockchain-crawlers-AWS_BITCOIN_MAINNET-Crawler
```

### Enable Scheduled Crawling (if needed)

| Schedule | Cron Expression | Est. Cost/Chain/Month |
|----------|-----------------|----------------------|
| Weekly | `cron(0 0 ? * SUN *)` | $0.12 |
| Daily | `cron(0 0 * * ? *)` | $0.50 |
| Hourly | `cron(0 * * * ? *)` | $1-2 |

```bash
aws glue update-crawler \
  --name blockchain-crawlers-AWS_BITCOIN_MAINNET-Crawler \
  --schedule "cron(0 0 ? * SUN *)"
```

---

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `S3Bucket` | aws-public-blockchain | Source S3 bucket |
| `SchemaVersion` | v1.0 | Schema version for BTC/ETH |
| `SchemaVersionTON` | v1.1 | Schema version for TON |
| `DiscoverySchedule` | Weekly (Sunday 2AM) | How often to scan for new blockchains |
| `DefaultCrawlerSchedule` | disabled | Default crawler schedule (partition projection handles new partitions) |
| `EnableAutoCrawling` | true | Enable/disable automatic scheduling |

---

## Documentation

- [Crawler Design](../docs/CRAWLER_DESIGN.md) - Architecture and implementation details
- [Crawler User Guide](../docs/CRAWLER_README.md) - Detailed usage and troubleshooting
