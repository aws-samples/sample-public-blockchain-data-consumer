# AWS Glue Crawler User Guide
## Automatic Blockchain Schema Discovery

Automatically discovers and catalogs blockchain data from the AWS Public Blockchain S3 bucket using a hybrid approach: manifest-first discovery for authoritative chain naming, one-time Glue Crawlers for schema inference, and Athena Partition Projection for zero-cost automatic partition resolution.

**Key Benefits:**
- Zero manual schema definition
- Automatic discovery of new blockchains via [manifest.json](https://aws-public-blockchain.s3.us-east-2.amazonaws.com/manifest.json) with S3 heuristic fallback
- Manifest-based database naming (e.g., `aws_bitcoin_mainnet`, `aws_ethereum_mainnet`)
- One-time crawling per chain — partition projection handles new partitions automatically
- Near-zero ongoing cost (~$0.01/month after initial setup)
- Email notifications on discoveries and crawler completions

---
## CloudFormation Quick Create

<a href="https://us-east-2.console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/quickcreate?templateURL=https%3A%2F%2Faws-public-blockchain.s3.us-east-2.amazonaws.com%2Faws-public-blockchain-with-crawlers.yaml&stackName=blockchain-crawlers&param_S3Bucket=aws-public-blockchain&param_SchemaVersion=v1.0&param_DiscoverySchedule=cron(0%202%20%3F%20*%20SUN%20*)&param_EnableAutoCrawling=true&param_SchemaVersionTON=v1.1&param_DefaultCrawlerSchedule=daily"><img src="docs/static/quick-create-launch-stack.png" alt="Launch Stack" width="180"></a>

Click the button above to deploy with default settings. You'll need to:


1. Acknowledge IAM resource creation


2. Click **Create stack**
For custom configurations or CLI deployment, see [Quick Start](#quick-start) below.
---

## Quick Start

### Prerequisites

```bash
aws configure
```

You'll need an S3 bucket for deployment artifacts (Lambda code zips). Create one if you don't have one:

```bash
aws s3 mb s3://my-deployment-bucket
```

### 1. Deploy the Stack

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

The stack automatically runs initial discovery on deployment: fetches the manifest for chain names, creates databases and crawlers, runs each crawler once for schema inference, then enables partition projection for automatic partition discovery.

### 2. Subscribe to Notifications

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

### 3. Query Data

```sql
-- In Athena console (use the AWSPublicBlockchain workgroup)
SHOW DATABASES;
SHOW TABLES IN aws_bitcoin_mainnet;
SELECT * FROM aws_bitcoin_mainnet.blocks WHERE date = '2024-01-01' LIMIT 10;
SELECT * FROM aws_ethereum_mainnet.transactions WHERE date = '2024-01-01' LIMIT 10;
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
│  Runs once per chain to infer schema from Parquet metadata  │
│  Disabled after initial run — no ongoing crawler costs      │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              CrawlerCompletionHandler (Lambda)               │
│  - Adds partition projection properties to tables           │
│  - Sends SNS notification                                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                  AWS Glue Data Catalog                       │
│  Tables with partition projection enabled:                  │
│  aws_bitcoin_mainnet | aws_ethereum_mainnet | ton_mainnet   │
│  stellar_pubnet | sonarx_arbitrum_mainnet | etc.            │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                     Amazon Athena                            │
│  Calculates partitions in-memory (no Glue catalog lookups)  │
│  New date partitions discovered automatically               │
└─────────────────────────────────────────────────────────────┘
```

---

## Managing Crawlers

Crawlers run once on initial discovery to infer schemas. After that, partition projection handles new date partitions automatically. You only need to re-run crawlers if the schema changes (e.g., new columns added to the data).

### Manually Trigger a Crawler

```bash
aws glue start-crawler --name blockchain-crawlers-AWS_BITCOIN_MAINNET-Crawler
```

### Enable Scheduled Crawling (if needed)

```bash
# Set to weekly for periodic schema checks
aws glue update-crawler \
  --name blockchain-crawlers-AWS_BITCOIN_MAINNET-Crawler \
  --schedule "cron(0 0 ? * SUN *)"

# Disable schedule again
aws glue update-crawler \
  --name blockchain-crawlers-AWS_BITCOIN_MAINNET-Crawler \
  --schedule ""
```

### List All Crawlers

```bash
aws glue list-crawlers --query 'CrawlerNames[?starts_with(@, `blockchain-crawlers-`)]'
```

---

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `S3Bucket` | aws-public-blockchain | Source S3 bucket |
| `SchemaVersion` | v1.0 | Schema version for BTC/ETH |
| `SchemaVersionTON` | v1.1 | Schema version for TON and newer chains |
| `DiscoverySchedule` | Weekly (Sunday 2AM) | How often to scan for new blockchains |
| `DefaultCrawlerSchedule` | disabled | Default crawler schedule (partition projection handles new partitions) |
| `EnableAutoCrawling` | true | Enable/disable automatic discovery scheduling |

### Deploy with Custom Settings

```bash
aws cloudformation package \
  --template-file data-consumer/aws-public-blockchain-with-crawlers.yaml \
  --s3-bucket YOUR_DEPLOYMENT_BUCKET \
  --output-template-file packaged.yaml

aws cloudformation deploy \
  --template-file packaged.yaml \
  --stack-name blockchain-crawlers \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    DefaultCrawlerSchedule=weekly \
    DiscoverySchedule="cron(0 0 * * ? *)"
```

---

## Cost Considerations

| Component | Frequency | Est. Cost/Chain/Month |
|-----------|-----------|----------------------|
| Discovery Lambda | Weekly | ~$0.01 |
| Initial Crawler (per chain) | Once | ~$0.02 one-time |
| Ongoing Crawlers | None (disabled) | $0 |
| Partition Projection | Every query | $0 (in-memory) |

**Recommendations:**
- Default setup (partition projection) costs near-zero after initial crawl
- Only enable scheduled crawling if you need to track schema changes
- Weekly discovery Lambda is lightweight and catches new chains automatically

For detailed cost analysis, see [COST_ANALYSIS.md](../docs/COST_ANALYSIS.md).

---

## Troubleshooting

### Crawler Not Running

```bash
# Check crawler state
aws glue get-crawler --name blockchain-crawlers-BTC-Crawler \
  --query 'Crawler.{State:State,Schedule:Schedule}' --output json

# Check if schedule is set
aws glue get-crawler --name blockchain-crawlers-BTC-Crawler \
  --query 'Crawler.Schedule.ScheduleExpression' --output text
```

### No Tables After Crawler Run

```bash
# Check crawler logs
aws logs tail /aws-glue/crawlers --follow

# Verify S3 data exists
aws s3 ls s3://aws-public-blockchain/v1.0/btc/ --request-payer requester
```

### Discovery Not Finding New Chains

```bash
# Check discovery Lambda logs
aws logs tail /aws/lambda/blockchain-crawlers-BlockchainDiscovery --follow

# Manually trigger discovery
aws lambda invoke \
  --function-name blockchain-crawlers-BlockchainDiscovery \
  --payload '{}' \
  response.json --no-cli-pager && cat response.json
```

---

## Lambda Functions

| Function | Purpose |
|----------|---------|
| `BlockchainDiscovery` | Fetches manifest, discovers chains, creates DBs and crawlers |
| `CrawlerCompletionHandler` | Adds partition projection after crawler completes, sends notifications |
| `InitialDiscoveryTrigger` | Triggers discovery on stack creation, cleans up on deletion |

---

## Files

| File | Purpose |
|------|---------|
| `data-consumer/aws-public-blockchain-with-crawlers.yaml` | CloudFormation template |
| `data-consumer/lambda/discovery/index.py` | Discovery Lambda (manifest-first + heuristic fallback) |
| `data-consumer/lambda/completion/index.py` | Crawler completion handler (partition projection) |
| `data-consumer/lambda/cleanup/index.py` | Stack creation trigger / deletion cleanup |
| `utils/blockchain_schema_discovery.py` | Python utility for manual exploration |
| `docs/CRAWLER_DESIGN.md` | Architecture documentation |

---

## Least Privilege IAM Policy for Deployment

The following IAM policy provides the minimum permissions needed to deploy and manage this stack. Attach it to the IAM user or role used for CLI deployment.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudFormationStackManagement",
      "Effect": "Allow",
      "Action": [
        "cloudformation:CreateStack",
        "cloudformation:UpdateStack",
        "cloudformation:DeleteStack",
        "cloudformation:DescribeStacks",
        "cloudformation:DescribeStackEvents",
        "cloudformation:GetTemplate",
        "cloudformation:ListStacks",
        "cloudformation:CreateChangeSet",
        "cloudformation:DescribeChangeSet",
        "cloudformation:ExecuteChangeSet",
        "cloudformation:DeleteChangeSet"
      ],
      "Resource": "arn:aws:cloudformation:*:*:stack/blockchain-crawlers/*"
    },
    {
      "Sid": "LambdaManagement",
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration",
        "lambda:DeleteFunction",
        "lambda:GetFunction",
        "lambda:GetFunctionConfiguration",
        "lambda:AddPermission",
        "lambda:RemovePermission",
        "lambda:InvokeFunction",
        "lambda:TagResource"
      ],
      "Resource": "arn:aws:lambda:*:*:function:blockchain-crawlers-*"
    },
    {
      "Sid": "IAMRoleManagement",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:GetRole",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:GetRolePolicy",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:PassRole",
        "iam:TagRole"
      ],
      "Resource": [
        "arn:aws:iam::*:role/blockchain-crawlers-*"
      ]
    },
    {
      "Sid": "S3BucketManagement",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:GetBucketPolicy",
        "s3:PutBucketPolicy",
        "s3:DeleteBucketPolicy",
        "s3:PutEncryptionConfiguration",
        "s3:GetEncryptionConfiguration",
        "s3:PutBucketVersioning",
        "s3:GetBucketVersioning",
        "s3:PutBucketPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock"
      ],
      "Resource": "arn:aws:s3:::blockchain-crawlers-*"
    },
    {
      "Sid": "S3DeploymentBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::YOUR_DEPLOYMENT_BUCKET/*"
    },
    {
      "Sid": "AthenaWorkgroup",
      "Effect": "Allow",
      "Action": [
        "athena:CreateWorkGroup",
        "athena:DeleteWorkGroup",
        "athena:GetWorkGroup",
        "athena:UpdateWorkGroup",
        "athena:TagResource"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EventBridgeRules",
      "Effect": "Allow",
      "Action": [
        "events:PutRule",
        "events:DeleteRule",
        "events:DescribeRule",
        "events:PutTargets",
        "events:RemoveTargets"
      ],
      "Resource": "arn:aws:events:*:*:rule/blockchain-crawlers-*"
    },
    {
      "Sid": "SNSManagement",
      "Effect": "Allow",
      "Action": [
        "sns:CreateTopic",
        "sns:DeleteTopic",
        "sns:GetTopicAttributes",
        "sns:SetTopicAttributes",
        "sns:Subscribe",
        "sns:TagResource"
      ],
      "Resource": "arn:aws:sns:*:*:blockchain-crawlers-*"
    },
    {
      "Sid": "SSMParameters",
      "Effect": "Allow",
      "Action": [
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "ssm:GetParameter",
        "ssm:AddTagsToResource"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/blockchain-crawlers/*"
    },
    {
      "Sid": "KMSForSNS",
      "Effect": "Allow",
      "Action": [
        "kms:DescribeKey",
        "kms:CreateGrant"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "kms:ViaService": "sns.*.amazonaws.com"
        }
      }
    }
  ]
}
```

Replace `YOUR_DEPLOYMENT_BUCKET` with the S3 bucket used for `cloudformation package`. Replace `blockchain-crawlers` with your stack name if using a different name.
