# User Guide

## SAP S-API to BigQuery Extraction Solution

This guide covers day-to-day operations, monitoring, and troubleshooting of the BQ Toolkit Extractor.

---

## Overview

The solution extracts data from SAP S-API extractors (Logistics 2LIS_* and FI/CO 0FI_*/0CO_*) and replicates it to Google BigQuery using the BQ Toolkit.

### Key Features
- **Parameter-Driven Extraction**: Mass transfer key passed as input parameter
- **Auto-Configuration**: ZBQTR_CONFIG is auto-populated during initialization
- **Fail-Safe**: Failed extractions leave deltas unconfirmed for automatic retry
- **No BW Required**: Uses ODP API directly, no SAP BW dependency
- **Comprehensive Logging**: All operations logged to ZBQTR_LOG

---

## Quick Start: Setting Up a New Datasource

### Prerequisites

1. Mass Transfer Key created in `/GOOG/BQTR_SETTINGS`
2. BigQuery table created for target dataset
3. Service account configured with BigQuery permissions

### Step 1: Initialize the Datasource

Run the extractor program to initialize the ODP subscription and auto-register configuration:

```
Transaction: SA38 or SE38
Program: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds     = 2LIS_11_VAHDR    (exact datasource name, no wildcards)
  - p_mtkey  = ZMTR_SALES_HDR   (your BQ Toolkit mass transfer key)
  - p_struct = (optional)        (DDIC structure, derived if blank)
  - p_mode   = I                 (Initialize)
```

This will:
1. Create the ODP subscription for the datasource
2. Auto-populate ZBQTR_CONFIG with the configuration

### Step 2: Run Full Load

```
Parameters:
  - p_ds     = 2LIS_11_VAHDR
  - p_mtkey  = ZMTR_SALES_HDR
  - p_mode   = F                 (Full)
```

### Step 3: Schedule Delta Job (SM36)

Create a background job for each datasource:

1. **SM36** -> Define Background Job
2. Job Name: `Z_BQ_DELTA_2LIS_11_VAHDR`
3. Create step with program `Z_BQ_EXTRACTOR_RUN`
4. Create variant:
   | Parameter | Value | Description |
   |-----------|-------|-------------|
   | p_ds | 2LIS_11_VAHDR | Exact datasource name |
   | p_mtkey | ZMTR_SALES_HDR | Mass transfer key |
   | p_mode | D | Delta extraction |
5. Set periodic schedule (e.g., every 15 minutes)
6. Save and release

### That's It!

- Subsequent runs extract deltas and send to BigQuery
- BAdI automatically intercepts only our initialized subscriptions
- Configuration is stored in ZBQTR_CONFIG for reference

---

## Adding Multiple Datasources

For each datasource you want to replicate:

1. **Create Mass Transfer Key** in `/GOOG/BQTR_SETTINGS`
2. **Initialize** via Z_BQ_EXTRACTOR_RUN with mode = I
3. **Full Load** via Z_BQ_EXTRACTOR_RUN with mode = F
4. **Schedule Delta Job** in SM36

### Example: Complete Setup for Sales Datasources

```
Datasources: 2LIS_11_VAHDR, 2LIS_11_VAITM, 2LIS_12_VCHDR

Step 1 - Initialize each (run once):
  Program: Z_BQ_EXTRACTOR_RUN

  Variant INIT_VAHDR:
    p_ds     = 2LIS_11_VAHDR
    p_mtkey  = ZMTR_SALES_HDR
    p_mode   = I

  Variant INIT_VAITM:
    p_ds     = 2LIS_11_VAITM
    p_mtkey  = ZMTR_SALES_ITM
    p_mode   = I

  Variant INIT_VCHDR:
    p_ds     = 2LIS_12_VCHDR
    p_mtkey  = ZMTR_DELIVERY_HDR
    p_mode   = I

Step 2 - Full Load each (one-time):
  Same variants with p_mode = F

Step 3 - Delta Jobs (recurring):
  Create one job per datasource with p_mode = D
  Schedule: Every 15 minutes
```

---

## Extraction Modes Reference

| Mode | Description |
|------|-------------|
| D | Delta extraction (for existing subscriptions) |
| F | Full extraction (reload all data) |
| I | Initialize subscription + register config |
| R | Recovery (reset + full) |
| A | Auto (Init+Full if new, Delta if exists) |

---

## Program Parameters

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| p_ds | CHAR30 | Exact datasource name (no wildcards allowed) |
| p_mtkey | CHAR20 | BQ Toolkit Mass Transfer Key |

### Optional Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| p_struct | CHAR30 | DDIC structure name (derived if blank) |
| p_mode | CHAR1 | Extraction mode (D, F, I, R, A) - default D |
| p_test | CHAR1 | Test mode - no BQ write (checkbox) |
| p_conn | CHAR1 | Pre-check BQ connection (checkbox) |

### Important: No Wildcards

The `p_ds` parameter requires an **exact datasource name**. Wildcards (`*`, `%`) are not allowed:

```
VALID:   p_ds = 2LIS_11_VAHDR
INVALID: p_ds = 2LIS_*
INVALID: p_ds = *
```

This ensures each datasource has its own mass transfer key and configuration.

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

#### View Registered Datasources
```sql
SELECT datasource, mass_tr_key, init_date, init_by
FROM zbqtr_config
ORDER BY init_date DESC
```

### ODQMON Navigation

1. Transaction **ODQMON**
2. Select view:
   - **Queue** -> See all delta queues
   - **Subscription** -> Filter by subscriber
3. Filter by:
   - Subscriber Name = `ZBQTR_SUBSCRIBER`
   - Context = `SAPI`

---

## Common Operations

### Manual Delta Extraction

Run extraction outside scheduled job:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = 2LIS_11_VAHDR
  - p_mtkey = ZMTR_SALES_HDR
  - p_mode = D
```

### Manual Full Refresh

Reload all data for a datasource:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = 2LIS_11_VAHDR
  - p_mtkey = ZMTR_SALES_HDR
  - p_mode = F
```

**Warning**: This adds data to existing table. Truncate BigQuery table first if needed.

### Reset Subscription

Clear delta pointer and reinitialize:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = 2LIS_11_VAHDR
  - p_mtkey = ZMTR_SALES_HDR
  - p_mode = R (Recovery)
```

### Test Mode

Run extraction without sending to BigQuery:

```
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  - p_ds = 2LIS_11_VAHDR
  - p_mtkey = ZMTR_SALES_HDR
  - p_mode = D
  - p_test = X (checked)
```

Useful for testing ODP extraction flow.

---

## ZBQTR_CONFIG Table

The configuration table is **auto-populated** during initialization. You do not need to manually maintain it.

### Table Structure

| Field | Description |
|-------|-------------|
| DATASOURCE | Datasource name (key) |
| MASS_TR_KEY | BQ Toolkit Mass Transfer Key |
| STRUCT_NAME | DDIC structure name |
| SUBSCRIBER_TYPE | ODP subscriber type (ZBQTR) |
| SUBSCRIBER_NAME | ODP subscriber name |
| BATCH_SIZE | Max records per API call |
| FULL_ONLY | Full load only flag |
| INIT_DATE | Initialization date |
| INIT_TIME | Initialization time |
| INIT_BY | Initialized by user |

### Viewing Configuration

Use SE16/SE16N to view ZBQTR_CONFIG entries:
- Transaction: SE16
- Table: ZBQTR_CONFIG

---

## Troubleshooting

### Issue: "Wildcards not allowed" Error

**Cause**: The `p_ds` parameter contains `*` or `%`.

**Solution**: Specify the exact datasource name (e.g., `2LIS_11_VAHDR`).

### Issue: Extraction Keeps Failing

**Symptoms**: Same datasource fails repeatedly

**Diagnosis**:
1. Check ZBQTR_LOG for error message
2. Check Z_BQ_EXTRACTOR_MONITOR -> Failures view

**Common Causes**:

| Error | Cause | Solution |
|-------|-------|----------|
| "Connection failed" | Network/auth issue | Check service account, firewall |
| "Quota exceeded" | BigQuery API limit | Wait or request quota increase |
| "Mass transfer key required" | Missing p_mtkey | Provide mass transfer key |
| "Invalid credentials" | Service account key expired | Rotate key in /GOOG/BQTR_SETTINGS |

### Issue: BAdI Not Intercepting

**Symptoms**: Extractions run but data not sent to BigQuery

**Diagnosis**:
1. Check if datasource is in ZBQTR_CONFIG
2. Verify subscriber_type and subscriber_name match

**Solution**:
1. Re-run initialization (mode = I) to populate config
2. Verify BAdI implementation is active (SE19)

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

---

## Best Practices

### Scheduling
- Use 15-minute intervals for near real-time data
- Create one job per datasource for better isolation
- Monitor job runtime to avoid overlaps

### Configuration
- Use descriptive mass transfer key names (e.g., ZMTR_SALES_HDR)
- Test initialization in dialog mode before scheduling
- Keep mass transfer key consistent across runs

### Monitoring
- Review ZBQTR_LOG daily
- Set up alerts for consecutive failures
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
| SE16 | View ZBQTR_CONFIG entries |

### Getting Help
1. Check this guide and [TECHNICAL.md](TECHNICAL.md)
2. Review SAP Notes: 691154 (RSU5_SAPI_BADI), 2232584 (ODP)
3. Check Google Cloud documentation for BQ Toolkit
4. Contact your SAP Basis / Google Cloud team
