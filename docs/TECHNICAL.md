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
│  │  - Accepts p_ds (datasource) and p_mtkey (mass transfer key)      │    │
│  │  - Auto-populates ZBQTR_CONFIG during initialization               │    │
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
│                       │  - Checks ZBQTR_CONFIG                              │
│                       │  - Verifies subscriber identity                     │
│                       └─────────────────────┘                               │
│                                │                                            │
│                                │ Calls BQ Toolkit                           │
│                                ▼                                            │
│                       ┌─────────────────────┐                               │
│                       │  ZCL_BQ_REPLICATOR  │                               │
│                       │  Wrapper Class      │                               │
│                       │  - Accepts mass_tr_key                              │
│                       │  - Fallback to config                               │
│                       │  - Error handling                                   │
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

## Key Design Decisions

### Parameter-Driven Configuration

The solution uses input parameters instead of manual config table maintenance:

| Parameter | Purpose |
|-----------|---------|
| p_ds | Exact datasource name (no wildcards) |
| p_mtkey | BQ Toolkit Mass Transfer Key |
| p_struct | Optional DDIC structure name |

This approach:
- Ensures each datasource has explicit configuration
- Eliminates manual SM30 maintenance
- Ties extraction to specific BQ Toolkit settings

### Auto-Population of ZBQTR_CONFIG

During initialization (mode = I), the solution automatically populates ZBQTR_CONFIG:

```abap
" Auto-register in ZBQTR_CONFIG
DATA(ls_config) = VALUE zbqtr_config(
  datasource       = iv_datasource
  mass_tr_key      = mv_mass_tr_key
  struct_name      = mv_struct_name
  subscriber_type  = zcl_bq_odp_subscriber=>c_subscriber_type
  subscriber_name  = zcl_bq_odp_subscriber=>c_subscriber_name
  init_date        = sy-datum
  init_time        = sy-uzeit
  init_by          = sy-uname
).
MODIFY zbqtr_config FROM ls_config.
```

### BAdI Subscriber Verification

The BAdI implementation checks that extractions come from our initialized subscriptions:

```abap
" Check if datasource was initialized by us
SELECT SINGLE * FROM zbqtr_config
  WHERE datasource = @lv_datasource
  INTO @DATA(ls_config).

IF sy-subrc <> 0.
  RETURN.  " Not our datasource
ENDIF.

" Verify subscriber identity
IF ls_config-subscriber_type <> zcl_bq_odp_subscriber=>c_subscriber_type OR
   ls_config-subscriber_name <> zcl_bq_odp_subscriber=>c_subscriber_name.
  RETURN.  " Not our subscription
ENDIF.
```

---

## Database Objects

### ZBQTR_CONFIG

Configuration table auto-populated during initialization.

| Field | Type | Description |
|-------|------|-------------|
| MANDT | CLNT | Client (key) |
| DATASOURCE | CHAR(30) | S-API datasource name (key) |
| MASS_TR_KEY | CHAR(20) | BQ Toolkit mass transfer key |
| STRUCT_NAME | CHAR(30) | DDIC structure name |
| SUBSCRIBER_TYPE | CHAR(10) | ODP subscriber type (ZBQTR) |
| SUBSCRIBER_NAME | CHAR(30) | ODP subscriber name |
| BATCH_SIZE | INT4 | Max records per API call (0 = unlimited) |
| FULL_ONLY | CHAR(1) | X = skip delta extractions |
| INIT_DATE | DATS | Initialization date |
| INIT_TIME | TIMS | Initialization time |
| INIT_BY | UNAME | Initialized by user |

**Note**: This table is auto-populated and should not be manually maintained.

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
      IMPORTING
        iv_datasource  TYPE char30
        iv_mass_tr_key TYPE char20 OPTIONAL
        iv_struct_name TYPE char30 OPTIONAL
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

**Constructor Logic**:

```abap
" Try to get config from table (populated during init)
SELECT SINGLE * FROM zbqtr_config
  WHERE datasource = @iv_datasource
  INTO @ms_config.

IF sy-subrc <> 0.
  " Not in config - use provided parameters
  IF iv_mass_tr_key IS INITIAL.
    RAISE EXCEPTION TYPE zcx_bq_replication_failed
      EXPORTING iv_error_text = 'Mass transfer key required'.
  ENDIF.
  ms_config-mass_tr_key = iv_mass_tr_key.
  ms_config-struct_name = iv_struct_name.
ELSE.
  " Config found - override with provided parameters if supplied
  IF iv_mass_tr_key IS NOT INITIAL.
    ms_config-mass_tr_key = iv_mass_tr_key.
  ENDIF.
ENDIF.
```

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
      c_mode_recovery   TYPE char1 VALUE 'R',
      c_mode_auto       TYPE char1 VALUE 'A'.

    METHODS constructor
      IMPORTING
        iv_datasource  TYPE char30
        iv_mass_tr_key TYPE char20 OPTIONAL
        iv_struct_name TYPE char30 OPTIONAL.

    METHODS initialize_subscription
      RETURNING VALUE(rv_success) TYPE abap_bool
      RAISING zcx_bq_replication_failed.

    METHODS run_full
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    METHODS run_delta
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    METHODS run_auto
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    METHODS reset_subscription
      RETURNING VALUE(rv_success) TYPE abap_bool.

    METHODS subscription_exists
      RETURNING VALUE(rv_exists) TYPE abap_bool.

    METHODS get_mass_tr_key
      RETURNING VALUE(rv_mass_tr_key) TYPE char20.

    METHODS get_struct_name
      RETURNING VALUE(rv_struct_name) TYPE char30.
ENDCLASS.
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
| DATA_TRANSFORM | Intercepts extracted data, verifies subscriber, sends to BigQuery |
| HIER_TRANSFORM | Hierarchy extraction (not used) |

**DATA_TRANSFORM Logic**:

```abap
" Check if datasource was initialized by us
DATA(ls_config) = get_datasource_config( lv_datasource ).
IF ls_config IS INITIAL.
  RETURN.  " Not our datasource
ENDIF.

" Verify subscriber identity
IF ls_config-subscriber_type <> zcl_bq_odp_subscriber=>c_subscriber_type OR
   ls_config-subscriber_name <> zcl_bq_odp_subscriber=>c_subscriber_name.
  RETURN.  " Not our subscription
ENDIF.

" Replicate using stored config
lo_replicator = NEW zcl_bq_replicator(
  iv_datasource  = lv_datasource
  iv_mass_tr_key = ls_config-mass_tr_key
  iv_struct_name = ls_config-struct_name ).
```

---

## Program Reference

### Z_BQ_EXTRACTOR_RUN

Main extraction program.

**Selection Screen Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| p_ds | CHAR30 | Yes | Exact datasource name (no wildcards) |
| p_mtkey | CHAR20 | Yes | BQ Toolkit Mass Transfer Key |
| p_struct | CHAR30 | No | DDIC structure name |
| p_mode | CHAR1 | No | Extraction mode (default D) |
| p_test | CHAR1 | No | Test mode checkbox |
| p_conn | CHAR1 | No | Connection pre-check checkbox |

**Wildcard Validation**:

```abap
IF p_ds CA '*%'.
  MESSAGE e000(zbqtr) WITH 'Wildcards not allowed. Specify exact datasource name.'.
  RETURN.
ENDIF.
```

**Config Registration**:

```abap
METHOD register_datasource_config.
  DATA(ls_config) = VALUE zbqtr_config(
    datasource       = iv_datasource
    mass_tr_key      = mv_mass_tr_key
    struct_name      = mv_struct_name
    subscriber_type  = zcl_bq_odp_subscriber=>c_subscriber_type
    subscriber_name  = zcl_bq_odp_subscriber=>c_subscriber_name
    init_date        = sy-datum
    init_time        = sy-uzeit
    init_by          = sy-uname
  ).
  MODIFY zbqtr_config FROM ls_config.
  COMMIT WORK AND WAIT.
ENDMETHOD.
```

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
   → Auto-populates ZBQTR_CONFIG

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

Override `ZCL_IM_SAPI_BQ->get_datasource_config()` for custom logic:

```abap
" Example: Add additional validation
IF iv_datasource(4) = '2LIS' AND sy-sysid = 'PRD'.
  " Custom production-only logic
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
