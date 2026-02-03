# User Guide

## SAP S-API to BigQuery Extraction Solution

This guide covers day-to-day operations, monitoring, and troubleshooting of the BQ Toolkit Extractor.

---

## Overview

The solution extracts data from SAP S-API extractors (Logistics 2LIS_* and FI/CO 0FI_*/0CO_*) and replicates it to Google BigQuery using the BQ Toolkit.

### Key Features
- **Scheduled Delta Extraction**: Every 15 minutes via SM36
- **Fail-Safe**: Failed extractions leave deltas unconfirmed for automatic retry
- **No BW Required**: Uses ODP API directly, no SAP BW dependency
- **Comprehensive Logging**: All operations logged to ZBQTR_LOG

---

## Quick Start: Scheduling Extraction Jobs (Auto Mode)

The simplest way to set up extraction is using **Auto mode** (`A`), which
automatically handles both initialization and ongoing deltas.

### How Auto Mode Works

| Datasource State | Auto Mode Action |
|------------------|------------------|
| New (no subscription) | Initialize → Full Load |
| Existing (has subscription) | Delta Load |

### Step 1: Configure Datasources

1. SM30 → View `ZBQTR_CONFIG`
2. Add entries for each datasource with ACTIVE = 'X'

### Step 2: Create Single Scheduled Job

1. **SM36** → Define Background Job
2. Job Name: `Z_BQ_EXTRACTION`
3. Create step with program `Z_BQ_EXTRACTOR_RUN`
4. Create variant:
   | Parameter | Value | Description |
   |-----------|-------|-------------|
   | p_ds | * | All active datasources |
   | p_mode | A | Auto mode |
   | p_para | X | Parallel processing |
5. Set periodic schedule (e.g., every 15 minutes)
6. Save and release

### That's It!

- First run: Each datasource is initialized and fully loaded
- Subsequent runs: Delta extraction for all datasources
- New datasources added to ZBQTR_CONFIG are automatically picked up

---

## Alternative: Manual Full + Delta Extraction Jobs

This section shows how to set up automated extraction using separate initialization, full load, and delta jobs.

### Prerequisites

1. Datasources configured in ZBQTR_CONFIG (SM30)
2. Mass Transfer Keys created in `/GOOG/BQTR_SETTINGS`
3. BigQuery tables created for target datasets

### Step 1: Initialize Subscriptions (One-Time)

Before extracting data, initialize ODP subscriptions for your datasources.

**Option A: Initialize All Active Datasources**
```
Transaction: SA38 or SE38
Program: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds   = *
  - p_mode = I
```

**Option B: Initialize Specific Datasources**

Run multiple times with each datasource name:
```
p_ds   = 2LIS_11_VAHDR
p_mode = I
```

### Step 2: Schedule Full Load Job (One-Time)

Create a one-time background job for the initial full load.

1. **Transaction SM36** → Define Background Job
2. Job Name: `Z_BQ_FULL_LOAD_YYYYMMDD`
3. Click **Step** → Create step:
   - Program: `Z_BQ_EXTRACTOR_RUN`
   - Variant: Create new or use existing
4. **Create Variant** (if needed):
   | Parameter | Value | Description |
   |-----------|-------|-------------|
   | p_ds | * | All active datasources (or specific name) |
   | p_mode | F | Full extraction |
   | p_test | (blank) | Live mode |
   | p_para | X | Parallel processing (recommended) |
5. Click **Start Condition** → Immediate or schedule for off-peak hours
6. **Save** the job

**For Multiple Specific Datasources:**

Create a variant for each datasource or run with `p_ds = *` to process all active entries in ZBQTR_CONFIG.

### Step 3: Schedule Recurring Delta Job

Create a periodic job for ongoing delta extractions.

1. **Transaction SM36** → Define Background Job
2. Job Name: `Z_BQ_DELTA_EXTRACTION`
3. Click **Step** → Create step:
   - Program: `Z_BQ_EXTRACTOR_RUN`
   - Variant: Create or select delta variant
4. **Create Variant**:
   | Parameter | Value | Description |
   |-----------|-------|-------------|
   | p_ds | * | All active datasources |
   | p_mode | D | Delta extraction |
   | p_test | (blank) | Live mode |
   | p_para | X | Parallel processing |
5. Click **Start Condition** → **Date/Time** → Set start time
6. Check **Periodic Job** → Click **Period Values**:
   - Select **Minutes** → Enter: `15` (or desired interval)
   - Or select **Hourly** for larger intervals
7. **Save** the job

### Step 4: Verify Job Scheduling

1. **SM37** → Job Overview
   - Job name: `Z_BQ_*`
   - Status: Scheduled / Released
2. Confirm jobs appear with correct schedule

### Example: Complete Setup for Sales Datasources

```
Datasources: 2LIS_11_VAHDR, 2LIS_11_VAITM, 2LIS_12_VCHDR

Step 1 - Initialize (run once for each):
  Program: Z_BQ_EXTRACTOR_RUN
  Variant INIT_SALES:
    p_ds   = 2LIS_11_VAHDR
    p_mode = I
  (repeat for other datasources or use p_ds = * if all are in ZBQTR_CONFIG)

Step 2 - Full Load Job (one-time):
  Job: Z_BQ_FULL_SALES_20260203
  Variant FULL_SALES:
    p_ds   = *
    p_mode = F
    p_para = X
  Start: Immediate or scheduled

Step 3 - Delta Job (recurring):
  Job: Z_BQ_DELTA_ALL
  Variant DELTA_ALL:
    p_ds   = *
    p_mode = D
    p_para = X
  Schedule: Every 15 minutes
```

### Monitoring After Setup

1. **SM37**: Check job execution status
2. **Z_BQ_EXTRACTOR_MONITOR**: View extraction results and errors
3. **ODQMON**: Verify ODP subscriptions and delta queue status
4. **BigQuery Console**: Confirm data arriving in target tables

### Tips

- **Off-Peak Full Loads**: Schedule full loads during maintenance windows
- **Stagger Large Extractions**: For many datasources, split into multiple jobs
- **Parallel Processing**: Enable `p_para = X` for faster extraction
- **Test First**: Run with `p_test = X` to validate before live extraction

---

## Adding New Extractors

### Step 1: Identify the S-API Datasource

Common S-API datasources:

| Area | Datasource | Description |
|------|------------|-------------|
| **Logistics - Purchasing** | 2LIS_02_SCL | Schedule Line |
| | 2LIS_02_SGR | Goods Receipt |
| | 2LIS_02_ITM | Purchase Order Item |
| **Logistics - Sales** | 2LIS_11_VAHDR | Sales Order Header |
| | 2LIS_11_VAITM | Sales Order Item |
| | 2LIS_12_VCHDR | Delivery Header |
| **Logistics - Inventory** | 2LIS_03_BF | Goods Movements |
| | 2LIS_03_UM | Revaluations |
| **FI - General Ledger** | 0FI_GL_4 | GL Line Items |
| | 0FI_GL_10 | GL Totals |
| **FI - Accounts Receivable** | 0FI_AR_4 | AR Line Items |
| **FI - Accounts Payable** | 0FI_AP_4 | AP Line Items |
| **CO - Controlling** | 0CO_OM_CCA_1 | Cost Center Actuals |
| | 0CO_OM_OPA_1 | Order Actuals |

### Step 2: Configure Mass Transfer in BQ Toolkit

1. Transaction `/GOOG/BQTR_SETTINGS`
2. Create new Mass Transfer Key:
   - Identify the DDIC structure for the datasource
   - Configure target BigQuery dataset/table
   - Test the configuration

### Step 3: Add Configuration Entry

1. Transaction **SM30** → View `ZBQTR_CONFIG`
2. Add new entry:
   - DATASOURCE = datasource name
   - MASS_TR_KEY = from Step 2
   - STRUCT_NAME = DDIC structure
   - ACTIVE = 'X'

### Step 4: Initialize Subscription

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = <new datasource name>
  - p_mode = I (Initialize)
```

### Step 5: Run Full Load

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = <new datasource name>
  - p_mode = F (Full)
```

### Step 6: Verify

- Check ZBQTR_LOG for success
- Check BigQuery table for data
- Check ODQMON for subscription status

---

## Monitoring

### Monitor Program

Run `Z_BQ_EXTRACTOR_MONITOR` for comprehensive status:

| View | Description |
|------|-------------|
| **Log entries** | Recent extraction history with status |
| **Subscriptions** | ODP subscription status per datasource |
| **Pending deltas** | Unconfirmed deltas in ODQMON |
| **Failures** | Datasources with consecutive failures |

### Quick Checks

#### Last Successful Extraction
```sql
SELECT datasource, timestamp, records_replicated
FROM zbqtr_log
WHERE status = 'S'
ORDER BY timestamp DESC
```

#### Failed Extractions Today
```sql
SELECT datasource, timestamp, error_message
FROM zbqtr_log
WHERE status = 'E'
  AND timestamp >= 'today'
ORDER BY timestamp DESC
```

#### Consecutive Failures
```sql
SELECT datasource, consecutive_failures, last_error
FROM zbqtr_subsc
WHERE consecutive_failures > 0
ORDER BY consecutive_failures DESC
```

### ODQMON Navigation

1. Transaction **ODQMON**
2. Select view:
   - **Queue** → See all delta queues
   - **Subscription** → Filter by subscriber
3. Filter by:
   - Subscriber Name = `ZBQTR_SUBSCRIBER`
   - Context = `SAPI`

---

## Common Operations

### Extraction Modes Reference

| Mode | Description |
|------|-------------|
| D | Delta extraction (for existing subscriptions) |
| F | Full extraction (reload all data) |
| I | Initialize subscription only |
| R | Recovery (reset + full) |
| A | Auto (Init+Full if new, Delta if exists) |

### Manual Delta Extraction

Run extraction outside scheduled job:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = * (all) or specific datasource
  - p_mode = D
```

### Manual Full Refresh

Reload all data for a datasource:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = <datasource name>
  - p_mode = F
```

**Warning**: This adds data to existing table. Truncate BigQuery table first if needed.

### Reset Subscription

Clear delta pointer and reinitialize:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = <datasource name>
  - p_mode = R (Recovery)
```

### Disable Datasource

1. SM30 → ZBQTR_CONFIG
2. Set ACTIVE = ' ' (blank)
3. Save

Disabled datasources are skipped by scheduled job.

### Test Mode

Run extraction without sending to BigQuery:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = *
  - p_mode = D
  - p_test = X (checked)
```

Useful for testing ODP extraction flow.

---

## Troubleshooting

### Issue: Extraction Keeps Failing

**Symptoms**: Same datasource fails repeatedly, consecutive_failures increasing

**Diagnosis**:
1. Check ZBQTR_LOG for error message
2. Check Z_BQ_EXTRACTOR_MONITOR → Failures view

**Common Causes**:

| Error | Cause | Solution |
|-------|-------|----------|
| "Connection failed" | Network/auth issue | Check service account, firewall |
| "Quota exceeded" | BigQuery API limit | Wait or request quota increase |
| "Table not found" | BQ table missing | Create table or fix config |
| "Invalid credentials" | Service account key expired | Rotate key in /GOOG/BQTR_SETTINGS |

**Recovery**:
1. Fix underlying issue
2. Next scheduled run will automatically retry
3. On success, consecutive_failures resets to 0

### Issue: Delta Queue Growing

**Symptoms**: ODQMON shows large pending delta, data not appearing in BigQuery

**Diagnosis**:
1. Check if scheduled job is running (SM37)
2. Check ZBQTR_LOG for errors
3. Check ODQMON for unconfirmed requests

**Solution**:
1. Fix any extraction errors
2. Run delta manually if needed
3. If persistent, reset subscription (mode = R)

### Issue: Duplicate Data in BigQuery

**Symptoms**: Same records appear multiple times

**Possible Causes**:
1. Full load run without truncating table
2. ODP subscription was reset and reprocessed

**Prevention**:
- Use delta mode for scheduled extractions
- Truncate BQ table before intentional full loads
- Consider using BigQuery MERGE statements

### Issue: Missing Data

**Symptoms**: Transactions in SAP not appearing in BigQuery

**Diagnosis**:
1. Check if datasource is active in ZBQTR_CONFIG
2. Check if ODP subscription exists (ODQMON)
3. Check extraction log for the datasource

**Common Causes**:
- Delta queue not initialized
- Datasource configuration missing
- BAdI filter not matching

### Issue: Performance - Slow Extraction

**Symptoms**: Extraction takes very long, job runs overlap

**Solutions**:
1. Reduce BATCH_SIZE in ZBQTR_CONFIG
2. Enable parallel processing (p_para = X)
3. Consider splitting into multiple jobs by datasource
4. Check BigQuery streaming quotas

---

## Best Practices

### Scheduling
- Use 15-minute intervals for near real-time data
- Consider staggered schedules for high-volume datasources
- Monitor job runtime to avoid overlaps

### Configuration
- Set appropriate BATCH_SIZE (10000-50000 recommended)
- Use FULL_ONLY = 'X' for datasources without delta capability
- Keep BQ table schema aligned with DDIC structure

### Monitoring
- Review ZBQTR_LOG daily
- Set up alerts for consecutive_failures >= 3
- Monitor BigQuery data freshness

### Maintenance
- Clean up ZBQTR_LOG entries older than 30 days
- Review ODQMON for orphaned subscriptions
- Test recovery procedures periodically

---

## Support

### Log Locations
- **ZBQTR_LOG**: Replication history
- **SM21**: System log (for critical errors)
- **SM37**: Background job log
- **ODQMON**: ODP delta queue status

### Useful Transactions
| Transaction | Purpose |
|-------------|---------|
| SM37 | Check scheduled job status |
| SM21 | System log for errors |
| ODQMON | ODP delta queue monitoring |
| /GOOG/BQTR_SETTINGS | BQ Toolkit configuration |
| SE19 | BAdI implementation status |

### Getting Help
1. Check this guide and [TECHNICAL.md](TECHNICAL.md)
2. Review SAP Notes: 691154 (RSU5_SAPI_BADI), 2232584 (ODP)
3. Check Google Cloud documentation for BQ Toolkit
4. Contact your SAP Basis / Google Cloud team
