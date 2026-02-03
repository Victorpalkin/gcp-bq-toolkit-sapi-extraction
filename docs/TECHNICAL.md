# Technical Reference

## SAP S-API to BigQuery Extraction Solution

This document provides detailed technical information about the solution architecture, class design, and customization options.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SAP ECC 6.0 EHP7/8 (NW 7.40+)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SM36 JOB (Every 15 min)                                                   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  Z_BQ_EXTRACTOR_RUN (Main Program)                                 │    │
│  │  - Reads ZBQTR_CONFIG for enabled extractors                       │    │
│  │  - Loops through each datasource                                   │    │
│  │  - Calls ODP API to trigger extraction                             │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│       │                                                                     │
│       │ Triggers ODP Extraction                                            │
│       ▼                                                                     │
│  ┌──────────────┐     ┌─────────────────────┐     ┌─────────────────────┐  │
│  │  S-API       │     │  ODP Framework      │     │  ODQMON             │  │
│  │  Extractor   │────▶│  CL_ODQ_*           │────▶│  Delta Queue        │  │
│  │  2LIS_*/0FI_*│     │  Subscription       │     │  (ODQDATA Table)    │  │
│  └──────────────┘     └─────────────────────┘     └─────────────────────┘  │
│                                │                            ▲               │
│                                │ Data Packages              │ Confirm/      │
│                                ▼                            │ Rollback      │
│                       ┌─────────────────────┐               │               │
│                       │  RSU5_SAPI_BADI     │───────────────┘               │
│                       │  ZCL_IM_SAPI_BQ     │                               │
│                       │  (DATA_TRANSFORM)   │                               │
│                       └─────────────────────┘                               │
│                                │                                            │
│                                │ Calls BQ Toolkit                           │
│                                ▼                                            │
│                       ┌─────────────────────┐                               │
│                       │  ZCL_BQ_REPLICATOR  │                               │
│                       │  Wrapper Class      │                               │
│                       │  - Error handling   │                               │
│                       │  - Logging          │                               │
│                       └─────────────────────┘                               │
│                                │                                            │
│                                │ /GOOG/CL_BQTR_DATA_LOAD->REPLICATE_DATA   │
│                                ▼                                            │
└────────────────────────────────┼────────────────────────────────────────────┘
                                 │ HTTPS (BigQuery API)
                                 ▼
                    ┌────────────────────────┐
                    │  Google Cloud BigQuery │
                    │  Dataset: sap_extract  │
                    │  Tables per extractor  │
                    └────────────────────────┘
```

---

## Database Objects

### ZBQTR_CONFIG

Configuration table for datasources.

| Field | Type | Description |
|-------|------|-------------|
| MANDT | CLNT | Client (key) |
| DATASOURCE | CHAR(30) | S-API datasource name (key) |
| MASS_TR_KEY | CHAR(20) | BQ Toolkit mass transfer key |
| STRUCT_NAME | CHAR(30) | DDIC structure name |
| BQ_DATASET | CHAR(100) | Target BigQuery dataset |
| BQ_TABLE | CHAR(100) | Target BigQuery table |
| ACTIVE | CHAR(1) | X = enabled |
| FULL_ONLY | CHAR(1) | X = skip delta extractions |
| BATCH_SIZE | INT4 | Max records per API call (0 = unlimited) |
| PARALLEL_GROUP | CHAR(10) | Server group for parallel processing |
| CREATED_BY | UNAME | Created by user |
| CREATED_ON | DATS | Creation date |
| CHANGED_BY | UNAME | Changed by user |
| CHANGED_ON | DATS | Change date |

### ZBQTR_LOG

Replication log table.

| Field | Type | Description |
|-------|------|-------------|
| MANDT | CLNT | Client (key) |
| LOG_ID | NUMC(20) | Unique log entry ID (key) |
| TIMESTAMP | TIMESTAMPL | Extraction timestamp |
| DATASOURCE | CHAR(30) | Datasource name |
| UPDMODE | CHAR(1) | F=Full, D=Delta, R=Recovery, I=Init |
| RECORDS_EXTRACTED | INT4 | Records from SAP |
| RECORDS_REPLICATED | INT4 | Records sent to BigQuery |
| STATUS | CHAR(1) | S=Success, E=Error, W=Warning |
| ERROR_MESSAGE | STRING | Error details |
| REQUEST_ID | CHAR(32) | ODP request pointer |
| DURATION_MS | INT4 | Processing time in milliseconds |
| JOB_NAME | CHAR(32) | Background job name |
| JOB_COUNT | NUMC(8) | Background job count |

### ZBQTR_SUBSC

ODP subscription tracking table.

| Field | Type | Description |
|-------|------|-------------|
| MANDT | CLNT | Client (key) |
| DATASOURCE | CHAR(30) | Datasource name (key) |
| SUBSCRIBER_ID | CHAR(30) | ODP subscriber name |
| INIT_DATE | DATS | Initialization date |
| INIT_TIME | TIMS | Initialization time |
| LAST_DELTA_DATE | DATS | Last delta extraction date |
| LAST_DELTA_TIME | TIMS | Last delta extraction time |
| LAST_DELTA_POINTER | CHAR(32) | Last ODP pointer |
| LAST_RECORDS | INT4 | Records in last extraction |
| TOTAL_RECORDS | INT8 | Cumulative records replicated |
| STATUS | CHAR(1) | A=Active, I=Inactive, E=Error |
| CONSECUTIVE_FAILURES | INT4 | Failure count (resets on success) |
| LAST_ERROR | STRING | Last error message |

---

## Class Reference

### ZCX_BQ_REPLICATION_FAILED

Custom exception class for replication failures.

```abap
CLASS zcx_bq_replication_failed DEFINITION
  INHERITING FROM cx_static_check.

  PUBLIC SECTION.
    DATA: mv_datasource TYPE char30,
          mv_error_code TYPE i,
          mv_error_text TYPE string,
          mv_request_id TYPE char32.

    METHODS constructor
      IMPORTING
        iv_datasource TYPE char30 OPTIONAL
        iv_error_code TYPE i OPTIONAL
        iv_error_text TYPE string OPTIONAL
        iv_request_id TYPE char32 OPTIONAL
        textid        LIKE if_t100_message=>t100key OPTIONAL
        previous      TYPE REF TO cx_root OPTIONAL.

    METHODS get_error_message
      RETURNING VALUE(rv_message) TYPE string.
ENDCLASS.
```

**Message IDs**:
| ID | Description |
|----|-------------|
| bq_api_error | BigQuery API call failed |
| odp_open_error | ODP OPEN function failed |
| odp_fetch_error | ODP FETCH function failed |
| odp_close_error | ODP CLOSE function failed |
| config_not_found | Datasource not in ZBQTR_CONFIG |
| bq_connection_error | Cannot connect to BigQuery |

### ZCL_BQ_REPLICATOR

Wrapper for BQ Toolkit data replication.

```abap
CLASS zcl_bq_replicator DEFINITION.
  PUBLIC SECTION.
    TYPES: BEGIN OF ty_result,
             success          TYPE abap_bool,
             error_code       TYPE i,
             error_message    TYPE string,
             records_sent     TYPE i,
             records_failed   TYPE i,
             duration_ms      TYPE i,
             request_id       TYPE char32,
           END OF ty_result.

    METHODS constructor
      IMPORTING iv_datasource TYPE char30
      RAISING zcx_bq_replication_failed.

    METHODS replicate
      IMPORTING it_data       TYPE STANDARD TABLE
                iv_updmode    TYPE char1
                iv_request_id TYPE char32 OPTIONAL
      RETURNING VALUE(rs_result) TYPE ty_result
      RAISING zcx_bq_replication_failed.

    METHODS test_connection
      RETURNING VALUE(rv_connected) TYPE abap_bool.

    METHODS get_config
      RETURNING VALUE(rs_config) TYPE zbqtr_config.
ENDCLASS.
```

**Key Behaviors**:
- Loads configuration from ZBQTR_CONFIG on construction
- Creates BQ Toolkit instance (/GOOG/CL_BQTR_DATA_LOAD)
- Supports batch processing via BATCH_SIZE config
- Logs all operations to ZBQTR_LOG
- Raises exception on any failure (fail-safe behavior)

### ZCL_BQ_ODP_SUBSCRIBER

ODP subscription manager.

```abap
CLASS zcl_bq_odp_subscriber DEFINITION.
  PUBLIC SECTION.
    CONSTANTS:
      c_subscriber_type TYPE char10 VALUE 'ZBQTR',
      c_subscriber_name TYPE char30 VALUE 'ZBQTR_SUBSCRIBER',
      c_context_sapi    TYPE char10 VALUE 'SAPI',
      c_mode_full       TYPE char1 VALUE 'F',
      c_mode_delta      TYPE char1 VALUE 'D',
      c_mode_init       TYPE char1 VALUE 'I',
      c_mode_recovery   TYPE char1 VALUE 'R'.

    METHODS constructor
      IMPORTING iv_datasource TYPE char30.

    METHODS initialize_subscription
      RETURNING VALUE(rv_success) TYPE abap_bool
      RAISING zcx_bq_replication_failed.

    METHODS run_full
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    METHODS run_delta
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    METHODS reset_subscription
      RETURNING VALUE(rv_success) TYPE abap_bool.

    METHODS subscription_exists
      RETURNING VALUE(rv_exists) TYPE abap_bool.
ENDCLASS.
```

**ODP API Usage**:
```abap
" Open extraction
CALL FUNCTION 'RODPS_REPL_ODP_OPEN'
  EXPORTING
    i_subscriber_type    = 'ZBQTR'
    i_subscriber_name    = 'ZBQTR_SUBSCRIBER'
    i_context            = 'SAPI'
    i_odpname            = <datasource>
    i_extraction_mode    = 'D'  " or 'F'
    i_explicit_close     = abap_true
  IMPORTING
    e_pointer            = lv_pointer.

" Fetch data (loop)
CALL FUNCTION 'RODPS_REPL_ODP_FETCH'
  EXPORTING
    i_pointer        = lv_pointer
    i_maxpackagesize = 52428800  " 50 MB
  IMPORTING
    e_no_more_data   = lv_done
  TABLES
    et_data          = lt_data.

" Close with confirmation
CALL FUNCTION 'RODPS_REPL_ODP_CLOSE'
  EXPORTING
    i_pointer   = lv_pointer
    i_confirmed = abap_true.  " or abap_false to rollback
```

### ZCL_IM_SAPI_BQ

BAdI implementation for RSU5_SAPI_BADI.

```abap
CLASS zcl_im_sapi_bq DEFINITION.
  PUBLIC SECTION.
    INTERFACES if_ex_rsu5_sapi_badi.
ENDCLASS.
```

**Interface Methods**:

| Method | Description |
|--------|-------------|
| DATA_TRANSFORM | Intercepts extracted data, sends to BigQuery |
| HIER_TRANSFORM | Hierarchy extraction (not used) |

**DATA_TRANSFORM Parameters**:
| Parameter | Direction | Description |
|-----------|-----------|-------------|
| I_DATASOURCE | Importing | Datasource name |
| I_UPDMODE | Importing | F=Full, D=Delta, R=Repeat, I=Init |
| I_T_SELECT | Importing | Selection parameters |
| I_T_FIELDS | Importing | Field list |
| C_T_DATA | Changing | Data table |
| C_T_MESSAGES | Changing | Messages |

---

## ODP Framework Integration

### Function Modules

| Function | Purpose |
|----------|---------|
| RODPS_REPL_ODP_OPEN | Create/open subscription, start extraction |
| RODPS_REPL_ODP_FETCH | Fetch data packages |
| RODPS_REPL_ODP_CLOSE | Close extraction, confirm or cancel delta |
| RODPS_REPL_ODP_RESET | Reset subscription (clear delta pointer) |

### Subscription Lifecycle

```
1. INITIALIZE (First time)
   RODPS_REPL_ODP_OPEN (mode = 'I')
   RODPS_REPL_ODP_CLOSE (confirmed = true)
   → Creates subscription in ODQMON

2. DELTA EXTRACTION (Regular)
   RODPS_REPL_ODP_OPEN (mode = 'D')
   RODPS_REPL_ODP_FETCH (loop until no_more_data)
   → BAdI intercepts data, sends to BQ
   RODPS_REPL_ODP_CLOSE (confirmed = true/false)
   → Confirms delta if successful

3. FULL LOAD (On-demand)
   RODPS_REPL_ODP_OPEN (mode = 'F')
   RODPS_REPL_ODP_FETCH (loop)
   RODPS_REPL_ODP_CLOSE (confirmed = true)
   → Does not affect delta pointer

4. RECOVERY
   RODPS_REPL_ODP_RESET
   → Clears delta pointer
   RODPS_REPL_ODP_OPEN (mode = 'F')
   → Restart from scratch
```

### ODQMON Tables

| Table | Description |
|-------|-------------|
| ODQSN | Subscriptions |
| ODQREQUESTH | Request headers |
| ODQDATA | Delta data (actual records) |

---

## Error Handling

### Fail-Safe Pattern

The solution implements fail-safe extraction:

```abap
TRY.
    open_extraction( 'D' ).
    fetch_and_process( ).  " BAdI intercepts, sends to BQ
    close_extraction( abap_true ).  " Confirm delta

  CATCH zcx_bq_replication_failed.
    close_extraction( abap_false ).  " Do NOT confirm
    " Delta remains in queue for retry
    RAISE EXCEPTION lx_error.
ENDTRY.
```

### Exception Propagation

```
BQ API Error
    ↓
ZCL_BQ_REPLICATOR raises ZCX_BQ_REPLICATION_FAILED
    ↓
ZCL_IM_SAPI_BQ (BAdI) catches and re-raises
    ↓
ODP Framework receives exception
    ↓
ODP does NOT confirm delta
    ↓
Delta remains in ODQMON for retry
```

### Consecutive Failure Tracking

- `ZBQTR_SUBSC.CONSECUTIVE_FAILURES` increments on each failure
- Resets to 0 on success
- Alert triggered when threshold (3) exceeded
- Logged to SM21 system log

---

## Performance Considerations

### Batch Size

Configure `ZBQTR_CONFIG.BATCH_SIZE` to control records per API call:

| Batch Size | Use Case |
|------------|----------|
| 0 | Unlimited (for small extractions) |
| 10,000 | Standard recommendation |
| 5,000 | If BigQuery quota issues |
| 50,000 | For high-volume, stable connections |

### Parallel Processing

Enable parallel processing for multiple datasources:

```abap
Run: Z_BQ_EXTRACTOR_RUN
Parameters:
  p_para = X
  p_pgrp = ZBQTR  " Server group
```

Uses SPTA framework for parallel execution.

### Memory Optimization

- ODP FETCH uses `i_maxpackagesize = 52428800` (50 MB)
- Data processed in packages, not all at once
- BAdI called per package, not per record

---

## Customization Points

### Adding Custom Fields

1. Extend DDIC structure (e.g., MC02M_0GRTP)
2. Update BQ Toolkit mass transfer configuration
3. Alter BigQuery table schema

### Custom Filtering

Override `ZCL_IM_SAPI_BQ->is_active_datasource()` for custom logic:

```abap
" Example: Filter by logical system
IF iv_datasource(4) = '2LIS' AND sy-sysid = 'PRD'.
  rv_active = abap_true.
ENDIF.
```

### Custom Transformation

Extend BAdI method to transform data before sending:

```abap
" In DATA_TRANSFORM method
" Modify C_T_DATA before calling replicator
LOOP AT c_t_data ASSIGNING FIELD-SYMBOL(<row>).
  " Custom transformation logic
ENDLOOP.
```

### Custom Alerting

Extend `ZCL_BQ_ODP_SUBSCRIBER->record_failure()`:

```abap
" Send email on critical failures
IF ls_sub-consecutive_failures >= 3.
  CALL FUNCTION 'SO_NEW_DOCUMENT_SEND_API1'
    ...
ENDIF.
```

---

## SAP Notes

| Note | Description |
|------|-------------|
| 691154 | RSU5_SAPI_BADI documentation |
| 1931427 | ODP Replication API 2.0 |
| 2232584 | BS_ANLY_DS_RELEASE_ODP (datasource activation) |
| 2768678 | ODP performance optimizations |

---

## Related Documentation

- [ABAP SDK for Google Cloud - On-premises](https://cloud.google.com/solutions/sap/docs/abap-sdk/on-premises-or-any-cloud)
- [BQ Toolkit for SAP](https://cloud.google.com/solutions/sap/docs/abap-sdk/on-premises-or-any-cloud/latest/bq-toolkit-for-sap-overview)
- [ODP Delta Queue Anatomy](https://nttdata-solutions.com/uk/blog/anatomy-of-the-operational-delta-queues-in-sap-odp-extractors/)
