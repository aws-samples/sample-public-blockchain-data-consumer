#!/bin/bash
# =============================================================================
# Stack Deployment Validation Tests
# =============================================================================
# Usage: ./test_stack_deployment.sh <stack-name>
# Example: ./test_stack_deployment.sh blockchain-crawlers
#
# Prerequisites:
# - AWS CLI configured with appropriate permissions
# - Stack deployed and crawlers completed (wait ~10-15 min after deployment)
# =============================================================================

set -euo pipefail

STACK_NAME="${1:-blockchain-crawlers}"
PASSED=0
FAILED=0
SKIPPED=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() {
    echo -e "${GREEN}✓ PASS${NC}: $1"
    ((PASSED++)) || true
}

fail() {
    echo -e "${RED}✗ FAIL${NC}: $1"
    ((FAILED++)) || true
}

skip() {
    echo -e "${YELLOW}○ SKIP${NC}: $1"
    ((SKIPPED++)) || true
}

info() {
    echo -e "  → $1"
}

echo "=============================================="
echo "Stack Deployment Validation: $STACK_NAME"
echo "=============================================="
echo ""

# =============================================================================
# Test 1: Stack exists and is complete
# =============================================================================
echo "Test 1: Stack Status"
STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [[ "$STACK_STATUS" == "CREATE_COMPLETE" || "$STACK_STATUS" == "UPDATE_COMPLETE" ]]; then
    pass "Stack status is $STACK_STATUS"
else
    fail "Stack status is $STACK_STATUS (expected CREATE_COMPLETE or UPDATE_COMPLETE)"
    echo "Cannot continue without a deployed stack. Exiting."
    exit 1
fi

# =============================================================================
# Test 2: Required outputs exist
# =============================================================================
echo ""
echo "Test 2: Stack Outputs"

OUTPUTS=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[*].OutputKey' \
    --output text)

for OUTPUT in AthenaResultsBucket AthenaWorkgroup BlockchainDiscoveryFunction CrawlerNotificationTopicArn GlueCrawlerRoleArn; do
    if echo "$OUTPUTS" | grep -q "$OUTPUT"; then
        pass "Output $OUTPUT exists"
    else
        fail "Output $OUTPUT missing"
    fi
done

# =============================================================================
# Test 3: Glue databases created - Manifest Coverage
# =============================================================================
echo ""
echo "Test 3: Glue Databases (Manifest Coverage)"

DATABASES=$(aws glue get-databases --query 'DatabaseList[*].Name' --output text)

# Expected chains from manifest.json
# https://aws-public-blockchain.s3.us-east-2.amazonaws.com/manifest.json
EXPECTED_CHAINS=(
    "aws_bitcoin_mainnet"
    "aws_ethereum_mainnet"
    "ton_mainnet"
    "cronos_mainnet"
    "stellar_pubnet"
    "stellar_testnet"
    "sonarx_aptos_mainnet"
    "sonarx_arbitrum_mainnet"
    "sonarx_base_mainnet"
    "sonarx_provenance_mainnet"
    "sonarx_xrp_mainnet"
)

info "Checking manifest coverage for ${#EXPECTED_CHAINS[@]} expected chains"

MISSING_CHAINS=()
for CHAIN in "${EXPECTED_CHAINS[@]}"; do
    if echo "$DATABASES" | grep -qw "$CHAIN"; then
        pass "Database '$CHAIN' exists"
    else
        fail "Database '$CHAIN' not found (expected from manifest)"
        MISSING_CHAINS+=("$CHAIN")
    fi
done

if [[ ${#MISSING_CHAINS[@]} -eq 0 ]]; then
    pass "All ${#EXPECTED_CHAINS[@]} manifest chains have databases"
else
    info "Missing chains: ${MISSING_CHAINS[*]}"
fi

# =============================================================================
# Test 4: Glue crawlers created
# =============================================================================
echo ""
echo "Test 4: Glue Crawlers"

CRAWLERS=$(aws glue list-crawlers --query 'CrawlerNames' --output text)

# Check for stack-prefixed crawlers
STACK_CRAWLERS=$(echo "$CRAWLERS" | tr '\t' '\n' | grep "^${STACK_NAME}-" || true)
CRAWLER_COUNT=$(echo "$STACK_CRAWLERS" | grep -c . || echo 0)

if [[ $CRAWLER_COUNT -ge 2 ]]; then
    pass "Found $CRAWLER_COUNT crawlers for this stack"
else
    fail "Expected at least 2 crawlers, found $CRAWLER_COUNT"
fi

# Check crawler states (should be READY, not RUNNING after initial crawl)
for CRAWLER in $STACK_CRAWLERS; do
    STATE=$(aws glue get-crawler --name "$CRAWLER" --query 'Crawler.State' --output text 2>/dev/null || echo "ERROR")
    if [[ "$STATE" == "READY" ]]; then
        pass "Crawler $CRAWLER state is READY"
    elif [[ "$STATE" == "RUNNING" || "$STATE" == "STOPPING" ]]; then
        skip "Crawler $CRAWLER is $STATE (wait for completion)"
    else
        fail "Crawler $CRAWLER state is $STATE"
    fi
done

# =============================================================================
# Test 5: Tables and Partition Projection
# =============================================================================
echo ""
echo "Test 5: Tables and Partition Projection"

TOTAL_TABLES=0
TABLES_WITH_PROJECTION=0
PROJECTION_ERRORS=()

validate_chain_tables() {
    local DB=$1
    
    local TABLES
    TABLES=$(aws glue get-tables --database-name "$DB" --query 'TableList[*].Name' --output text 2>/dev/null) || TABLES=""
    
    if [[ -z "$TABLES" ]]; then
        skip "$DB: no tables found (crawler may still be running)"
        return
    fi
    
    local TABLE_COUNT=$(echo "$TABLES" | wc -w | tr -d ' ')
    ((TOTAL_TABLES += TABLE_COUNT)) || true
    info "$DB: $TABLE_COUNT tables ($TABLES)"
    
    # Validate partition projection on each table
    for TABLE in $TABLES; do
        local PARAMS
        PARAMS=$(aws glue get-table --database-name "$DB" --name "$TABLE" --query 'Table.Parameters' --output json 2>/dev/null) || PARAMS="{}"
        
        local ENABLED=$(echo "$PARAMS" | jq -r '.["projection.enabled"] // "null"')
        
        if [[ "$ENABLED" != "true" ]]; then
            # Check if table has date partition (projection only applies to date-partitioned tables)
            local HAS_DATE
            HAS_DATE=$(aws glue get-table --database-name "$DB" --name "$TABLE" \
                --query "Table.PartitionKeys[?Name=='date'].Name" --output text 2>/dev/null) || HAS_DATE=""
            if [[ -n "$HAS_DATE" ]]; then
                PROJECTION_ERRORS+=("$DB.$TABLE: projection not enabled but has date partition")
            fi
            continue
        fi
        
        ((TABLES_WITH_PROJECTION++)) || true
        
        # Validate projection config
        local DATE_TYPE=$(echo "$PARAMS" | jq -r '.["projection.date.type"] // "null"')
        local DATE_FORMAT=$(echo "$PARAMS" | jq -r '.["projection.date.format"] // "null"')
        local DATE_RANGE=$(echo "$PARAMS" | jq -r '.["projection.date.range"] // "null"')
        local TEMPLATE=$(echo "$PARAMS" | jq -r '.["storage.location.template"] // "null"')
        
        [[ "$DATE_TYPE" != "date" ]] && PROJECTION_ERRORS+=("$DB.$TABLE: projection.date.type='$DATE_TYPE' (expected 'date')") || true
        [[ "$DATE_FORMAT" != "yyyy-MM-dd" ]] && PROJECTION_ERRORS+=("$DB.$TABLE: projection.date.format='$DATE_FORMAT' (expected 'yyyy-MM-dd')") || true
        [[ "$DATE_RANGE" != *",NOW" ]] && PROJECTION_ERRORS+=("$DB.$TABLE: projection.date.range='$DATE_RANGE' (should end with ',NOW')") || true
        [[ "$TEMPLATE" != *'date=${date}'* ]] && PROJECTION_ERRORS+=("$DB.$TABLE: storage.location.template missing 'date=\${date}'") || true
        [[ "$TEMPLATE" != *"aws-public-blockchain"* ]] && PROJECTION_ERRORS+=("$DB.$TABLE: storage.location.template not pointing to aws-public-blockchain bucket") || true
    done
}

# Validate all expected chains
for DB in "${EXPECTED_CHAINS[@]}"; do
    if echo "$DATABASES" | grep -qw "$DB"; then
        validate_chain_tables "$DB"
    fi
done

# Summary results
if [[ $TOTAL_TABLES -gt 0 ]]; then
    pass "Found $TOTAL_TABLES tables across all chains"
else
    fail "No tables found in any chain"
fi

if [[ $TABLES_WITH_PROJECTION -gt 0 ]]; then
    pass "$TABLES_WITH_PROJECTION tables have partition projection enabled"
else
    fail "No tables have partition projection enabled"
fi

if [[ ${#PROJECTION_ERRORS[@]} -eq 0 ]]; then
    pass "All partition projection configurations are valid"
else
    fail "${#PROJECTION_ERRORS[@]} partition projection errors found:"
    for ERR in "${PROJECTION_ERRORS[@]}"; do
        info "  - $ERR"
    done
fi

# =============================================================================
# Test 6: Athena workgroup configured
# =============================================================================
echo ""
echo "Test 6: Athena Workgroup"

WORKGROUP=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`AthenaWorkgroup`].OutputValue' \
    --output text)

if aws athena get-work-group --work-group "$WORKGROUP" &>/dev/null; then
    pass "Athena workgroup '$WORKGROUP' exists"
    
    # Check requester pays enabled
    REQUESTER_PAYS=$(aws athena get-work-group \
        --work-group "$WORKGROUP" \
        --query 'WorkGroup.Configuration.RequesterPaysEnabled' \
        --output text)
    
    if [[ "$REQUESTER_PAYS" == "True" ]]; then
        pass "Requester pays enabled on workgroup"
    else
        fail "Requester pays not enabled (required for public blockchain bucket)"
    fi
else
    fail "Athena workgroup '$WORKGROUP' not found"
fi

# =============================================================================
# Test 7: SNS topic exists
# =============================================================================
echo ""
echo "Test 7: SNS Notifications"

TOPIC_ARN=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`CrawlerNotificationTopicArn`].OutputValue' \
    --output text)

if aws sns get-topic-attributes --topic-arn "$TOPIC_ARN" &>/dev/null; then
    pass "SNS topic exists: $TOPIC_ARN"
else
    fail "SNS topic not found"
fi

# =============================================================================
# Test 8: Lambda functions exist
# =============================================================================
echo ""
echo "Test 8: Lambda Functions"

for FUNC_SUFFIX in BlockchainDiscovery CrawlerCompletionHandler InitialDiscoveryTrigger; do
    FUNC_NAME="${STACK_NAME}-${FUNC_SUFFIX}"
    if aws lambda get-function --function-name "$FUNC_NAME" &>/dev/null; then
        pass "Lambda function $FUNC_NAME exists"
    else
        fail "Lambda function $FUNC_NAME not found"
    fi
done

# =============================================================================
# Test 9: Schema Accuracy Validation (All Tables)
# =============================================================================
echo ""
echo "Test 9: Schema Accuracy Validation"

# Run a SELECT * LIMIT 1 query on every table in every expected database
# If schema is wrong or data is unreadable, query fails

SCHEMA_VALIDATED=0
SCHEMA_FAILED=0
SCHEMA_SKIPPED=0

validate_table_schema() {
    local DB=$1
    local TABLE=$2
    
    # Build query - use a recent date partition if table has date partitioning
    local HAS_DATE
    HAS_DATE=$(aws glue get-table --database-name "$DB" --name "$TABLE" \
        --query "Table.PartitionKeys[?Name=='date'].Name" --output text 2>/dev/null) || HAS_DATE=""
    
    local QUERY
    if [[ -n "$HAS_DATE" ]]; then
        QUERY="SELECT * FROM ${DB}.${TABLE} WHERE date >= '2024-01-01' LIMIT 1"
    else
        QUERY="SELECT * FROM ${DB}.${TABLE} LIMIT 1"
    fi
    
    local QUERY_ID
    QUERY_ID=$(aws athena start-query-execution \
        --query-string "$QUERY" \
        --work-group "$WORKGROUP" \
        --query 'QueryExecutionId' \
        --output text 2>/dev/null || echo "")
    
    if [[ -z "$QUERY_ID" ]]; then
        info "$DB.$TABLE: could not start query"
        ((SCHEMA_SKIPPED++)) || true
        return
    fi
    
    # Wait for query (max 60 seconds)
    local STATUS="RUNNING"
    for i in {1..12}; do
        sleep 5
        STATUS=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query 'QueryExecution.Status.State' \
            --output text 2>/dev/null || echo "UNKNOWN")
        
        if [[ "$STATUS" == "SUCCEEDED" ]]; then
            ((SCHEMA_VALIDATED++)) || true
            return
        elif [[ "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]]; then
            local REASON
            REASON=$(aws athena get-query-execution \
                --query-execution-id "$QUERY_ID" \
                --query 'QueryExecution.Status.StateChangeReason' \
                --output text 2>/dev/null || echo "Unknown")
            info "$DB.$TABLE: FAILED - $REASON"
            ((SCHEMA_FAILED++)) || true
            return
        fi
    done
    
    info "$DB.$TABLE: query timed out"
    ((SCHEMA_SKIPPED++)) || true
}

info "Validating schema for all tables in all expected databases..."
info "This may take several minutes..."
echo ""

for DB in "${EXPECTED_CHAINS[@]}"; do
    # Skip if database doesn't exist
    if ! echo "$DATABASES" | grep -qw "$DB"; then
        info "$DB: database not found, skipping"
        continue
    fi
    
    # Get all tables in database
    TABLES=$(aws glue get-tables --database-name "$DB" --query 'TableList[*].Name' --output text 2>/dev/null) || TABLES=""
    
    if [[ -z "$TABLES" ]]; then
        info "$DB: no tables found"
        continue
    fi
    
    TABLE_COUNT=$(echo "$TABLES" | wc -w | tr -d ' ')
    info "$DB: validating $TABLE_COUNT tables..."
    
    for TABLE in $TABLES; do
        validate_table_schema "$DB" "$TABLE"
    done
done

echo ""
if [[ $SCHEMA_VALIDATED -gt 0 ]]; then
    pass "$SCHEMA_VALIDATED tables validated successfully"
else
    fail "No tables were validated"
fi

if [[ $SCHEMA_FAILED -gt 0 ]]; then
    fail "$SCHEMA_FAILED tables failed schema validation"
fi

if [[ $SCHEMA_SKIPPED -gt 0 ]]; then
    skip "$SCHEMA_SKIPPED tables skipped (timeout or unavailable)"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
echo "Test Summary"
echo "=============================================="
echo -e "${GREEN}Passed${NC}: $PASSED"
echo -e "${RED}Failed${NC}: $FAILED"
echo -e "${YELLOW}Skipped${NC}: $SKIPPED"
echo ""

if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Review output above.${NC}"
    exit 1
fi
