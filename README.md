# AWS Public Blockchain Data Consumer

Automatically discover and catalog blockchain data from the [AWS Public Blockchain Datasets](https://registry.opendata.aws/aws-public-blockchain/) using AWS Glue Crawlers. When new blockchains are added to the dataset, dedicated Glue databases are created, schemas are inferred, and tables become queryable via Amazon Athena.

**Key Features:**
- Zero manual schema definition — crawlers infer schemas from Parquet files
- Automatic discovery of new blockchains on a configurable schedule
- Separate Glue database per blockchain (btc, eth, ton, stellar_pubnet, etc.)
- Configurable crawler schedules (hourly, daily, weekly, or disabled)
- SNS notifications on discovery and crawler completion
- Automatic cleanup of Glue resources on stack deletion
- Estimated cost: ~$2-5/month for daily crawls

---

## Prerequisites

### AWS CLI
Install and configure the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) with credentials that have permissions to deploy CloudFormation stacks.

### Required IAM Permissions
The IAM user/role deploying the stack needs the following least-privilege policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudFormation",
      "Effect": "Allow",
      "Action": [
        "cloudformation:CreateStack",
        "cloudformation:UpdateStack",
        "cloudformation:DeleteStack",
        "cloudformation:DescribeStacks",
        "cloudformation:DescribeStackEvents",
        "cloudformation:GetTemplate"
      ],
      "Resource": "arn:aws:cloudformation:*:*:stack/blockchain-crawlers/*"
    },
    {
      "Sid": "IAM",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:GetRole",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:PassRole"
      ],
      "Resource": "arn:aws:iam::*:role/blockchain-crawlers-*"
    },
    {
      "Sid": "Lambda",
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:DeleteFunction",
        "lambda:GetFunction",
        "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration",
        "lambda:AddPermission",
        "lambda:RemovePermission",
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:blockchain-crawlers-*"
    },
    {
      "Sid": "S3",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:PutBucketPolicy",
        "s3:DeleteBucketPolicy",
        "s3:PutBucketVersioning",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::blockchain-crawlers-*"
    },
    {
      "Sid": "Glue",
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:DeleteDatabase",
        "glue:GetDatabase",
        "glue:CreateCrawler",
        "glue:DeleteCrawler",
        "glue:GetCrawler",
        "glue:StartCrawler",
        "glue:StopCrawler",
        "glue:UpdateCrawler"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Athena",
      "Effect": "Allow",
      "Action": [
        "athena:CreateWorkGroup",
        "athena:DeleteWorkGroup",
        "athena:GetWorkGroup",
        "athena:UpdateWorkGroup"
      ],
      "Resource": "arn:aws:athena:*:*:workgroup/AWSPublicBlockchain"
    },
    {
      "Sid": "SNS",
      "Effect": "Allow",
      "Action": [
        "sns:CreateTopic",
        "sns:DeleteTopic",
        "sns:GetTopicAttributes",
        "sns:SetTopicAttributes",
        "sns:Subscribe"
      ],
      "Resource": "arn:aws:sns:*:*:blockchain-crawlers-*"
    },
    {
      "Sid": "EventBridge",
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
      "Sid": "SSM",
      "Effect": "Allow",
      "Action": [
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "ssm:GetParameter"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/public-blockchain-*"
    }
  ]
}
```

**Note:** Replace `blockchain-crawlers` with your chosen stack name if different.

### AWS Region
Deploy in a region where AWS Glue and Athena are available. The `aws-public-blockchain` S3 bucket is in `us-east-1`, but cross-region access works (with potential data transfer costs).

---

## Quick Start

### 1. Deploy the Stack

```bash
aws cloudformation create-stack \
  --stack-name blockchain-crawlers \
  --template-body file://data-consumer/aws-public-blockchain-with-crawlers.yaml \
  --capabilities CAPABILITY_NAMED_IAM

# Wait for deployment to complete
aws cloudformation wait stack-create-complete --stack-name blockchain-crawlers
```

The stack automatically runs initial discovery on deployment, creating databases and crawlers for all blockchains found in S3.

### 2. Subscribe to Notifications (Optional)

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

Check your email and confirm the subscription.

### 3. Query Data in Athena

Once crawlers complete (check the Glue console or wait for SNS notification):

```sql
-- List all discovered databases
SHOW DATABASES;

-- List tables in a blockchain database
SHOW TABLES IN btc;

-- Query Bitcoin blocks
SELECT * FROM btc.blocks WHERE date = '2024-01-01' LIMIT 10;

-- Query Ethereum transactions
SELECT * FROM eth.transactions WHERE date = '2024-01-01' LIMIT 10;
```

Use the `AWSPublicBlockchain` workgroup in Athena for queries.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              AWS Public Blockchain S3 Bucket                 │
│                  (aws-public-blockchain)                     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│           BlockchainDiscoveryFunction (Lambda)               │
│  • Scans S3 for blockchain namespaces (v1.0/, v1.1/)        │
│  • Creates Glue database per blockchain                      │
│  • Creates Glue crawler per blockchain with schedule         │
│  • Handles nested structures (e.g., stellar/parquet/pubnet) │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              AWS Glue Crawlers (native scheduling)           │
│     BTC-Crawler | ETH-Crawler | TON-Crawler | ...           │
│       (daily)   |   (daily)   |   (daily)   |               │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   AWS Glue Data Catalog                      │
│           btc | eth | ton | stellar_pubnet | ...            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                      Amazon Athena                           │
│              (AWSPublicBlockchain workgroup)                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Managing Crawler Schedules

Crawlers use Glue's native scheduling. The default schedule is set at deployment time via the `DefaultCrawlerSchedule` parameter, but you can change individual crawler schedules via the AWS Console or CLI.

### Available Schedules

| Schedule | Cron Expression | Use Case |
|----------|-----------------|----------|
| Hourly | `cron(0 * * * ? *)` | Active development, frequent updates |
| Daily | `cron(0 0 * * ? *)` | Balanced cost/freshness (default) |
| Weekly | `cron(0 0 ? * SUN *)` | Cost-optimized, infrequent updates |
| Disabled | (empty) | Manual runs only |

### View Crawler Schedule

```bash
aws glue get-crawler --name blockchain-crawlers-BTC-Crawler \
  --query 'Crawler.Schedule' --output json
```

### Update Crawler Schedule

```bash
# Set to hourly
aws glue update-crawler \
  --name blockchain-crawlers-BTC-Crawler \
  --schedule "cron(0 * * * ? *)"

# Set to daily (midnight UTC)
aws glue update-crawler \
  --name blockchain-crawlers-ETH-Crawler \
  --schedule "cron(0 0 * * ? *)"

# Set to weekly (Sunday midnight UTC)
aws glue update-crawler \
  --name blockchain-crawlers-TON-Crawler \
  --schedule "cron(0 0 ? * SUN *)"
```

### Disable Schedule (Manual Only)

```bash
aws glue update-crawler \
  --name blockchain-crawlers-BTC-Crawler \
  --schedule ""
```

### Manually Trigger a Crawler

```bash
aws glue start-crawler --name blockchain-crawlers-BTC-Crawler
```

### List All Crawlers

```bash
aws glue list-crawlers --query 'CrawlerNames[?starts_with(@, `blockchain-crawlers-`)]'
```

---

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `S3Bucket` | `aws-public-blockchain` | Source S3 bucket |
| `SchemaVersion` | `v1.0` | Schema version for BTC/ETH |
| `SchemaVersionTON` | `v1.1` | Schema version for TON and newer chains |
| `DiscoverySchedule` | `cron(0 2 ? * SUN *)` | When to scan for new blockchains (weekly Sunday 2AM UTC) |
| `DefaultCrawlerSchedule` | `daily` | Default schedule for new crawlers (`hourly`, `daily`, `weekly`, `disabled`) |
| `EnableAutoCrawling` | `true` | Enable/disable automatic discovery scheduling |

### Deploy with Custom Settings

```bash
aws cloudformation create-stack \
  --stack-name blockchain-crawlers \
  --template-body file://data-consumer/aws-public-blockchain-with-crawlers.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=DefaultCrawlerSchedule,ParameterValue=hourly \
    ParameterKey=DiscoverySchedule,ParameterValue="cron(0 0 * * ? *)"
```

---

## Cost Considerations

| Schedule | Crawler Runs/Month | Est. Cost/Chain |
|----------|-------------------|-----------------|
| Hourly | 720 | $1-2 |
| Daily | 30 | $0.30-0.50 |
| Weekly | 4 | $0.05-0.10 |

**Base costs (regardless of schedule):**
- S3 bucket for Athena results: ~$0.023/GB stored
- SNS notifications: negligible
- Lambda invocations: negligible

**Recommendations:**
- Use `daily` for most chains (default) — good balance of freshness and cost
- Use `weekly` for cost-sensitive deployments or chains you rarely query
- Use `hourly` only during active development
- Disable schedules for chains you don't need

For detailed cost analysis, see [docs/COST_ANALYSIS.md](docs/COST_ANALYSIS.md).

---

## Cleanup

Delete the stack to remove all resources:

```bash
aws cloudformation delete-stack --stack-name blockchain-crawlers
aws cloudformation wait stack-delete-complete --stack-name blockchain-crawlers
```

The stack automatically cleans up:
- All Glue crawlers created by the stack
- All Glue databases and tables created by the stack
- EventBridge rules and Lambda functions

**Note:** The Athena results S3 bucket has `DeletionPolicy: Retain` and will not be deleted. Delete it manually if needed:

```bash
# List and delete the bucket (after stack deletion)
aws s3 rb s3://<athena-results-bucket-name> --force
```

---

## Troubleshooting

### Crawler Not Running

```bash
# Check crawler state and schedule
aws glue get-crawler --name blockchain-crawlers-BTC-Crawler \
  --query 'Crawler.{State:State,Schedule:Schedule}' --output json
```

### No Tables After Crawler Run

```bash
# Check crawler logs in CloudWatch
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
  response.json && cat response.json
```

### Stack Deletion Stuck

If stack deletion hangs, it may be waiting for crawlers to stop. Check and stop any running crawlers:

```bash
aws glue list-crawlers --query 'CrawlerNames[?starts_with(@, `blockchain-crawlers-`)]' --output text | \
  xargs -I {} aws glue stop-crawler --name {}
```

---

## Lambda Functions

| Function | Purpose |
|----------|---------|
| `BlockchainDiscovery` | Discovers blockchains in S3, creates Glue databases and crawlers |
| `InitialDiscoveryTrigger` | Runs discovery on stack creation, cleans up on stack deletion |
| `CrawlerCompletionHandler` | Sends SNS notifications when crawlers complete, deduplicates schemas |

---

## Project Structure

```
├── data-consumer/
│   └── aws-public-blockchain-with-crawlers.yaml  # CloudFormation template
├── docs/
│   ├── COST_ANALYSIS.md                          # Detailed cost breakdown
│   └── CRAWLER_DESIGN.md                         # Architecture documentation
├── utils/
│   ├── blockchain_schema_discovery.py            # Python utility for manual S3 exploration
│   └── requirements-discovery.txt                # Python dependencies
└── README.md                                      # This file
```

---

## Security

- All S3 buckets enforce HTTPS-only access
- S3 bucket for Athena results has public access blocked
- SNS topic uses AWS-managed KMS encryption
- IAM roles follow least-privilege principle
- Athena workgroup enforces encryption for query results

---

## License

This project is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
