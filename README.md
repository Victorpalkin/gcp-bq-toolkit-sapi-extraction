# SAP S-API to BigQuery Extraction Solution

A custom SAP extraction solution that replicates data from S-API extractors (Logistics 2LIS_* and FI/CO 0FI_*/0CO_*) to Google BigQuery using the BQ Toolkit.

## Features

- **Parameter-Driven Configuration**: Mass transfer key passed as input, no manual config maintenance
- **Auto-Configuration**: ZBQTR_CONFIG auto-populated during initialization
- **Direct ODP Integration**: No SAP BW required - uses ODP Replication API directly
- **Fail-Safe Design**: Failed extractions leave deltas unconfirmed for automatic retry
- **Subscriber Verification**: BAdI only intercepts our initialized subscriptions
- **Comprehensive Logging**: All operations tracked in ZBQTR_LOG

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SM36 Job (Every 15 min)                                        │
│       ↓                                                         │
│  Z_BQ_EXTRACTOR_RUN (p_ds, p_mtkey)                             │
│       ↓                                                         │
│  ZCL_BQ_ODP_SUBSCRIBER → ODP Framework → RSU5_SAPI_BADI         │
│       ↓                                                         │
│  ZCL_IM_SAPI_BQ (verifies subscriber) → ZCL_BQ_REPLICATOR       │
│       ↓                                                         │
│  BQ Toolkit → Google BigQuery                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- SAP ECC 6.0 EHP7/8 or S/4HANA (NetWeaver 7.40+)
- ABAP SDK for Google Cloud (On-premises edition) v1.9+
- BQ Toolkit installed and configured
- Google Cloud project with BigQuery API enabled
- Service account with BigQuery Data Editor role

## Installation

See [docs/INSTALL.md](docs/INSTALL.md) for detailed installation steps.

### Quick Start

1. **Install via abapGit**: Clone repository into package `ZBQTR`
2. **Configure Mass Transfer Key** in `/GOOG/BQTR_SETTINGS`
3. **Initialize datasource**:
   ```
   Z_BQ_EXTRACTOR_RUN
   - p_ds     = 2LIS_11_VAHDR     (exact datasource name)
   - p_mtkey  = ZMTR_SALES_HDR    (your mass transfer key)
   - p_mode   = I                  (initialize)
   ```
4. **Run full load**: Same parameters with `p_mode = F`
5. **Schedule delta job in SM36**: Same parameters with `p_mode = D`

### Important: No Wildcards

The `p_ds` parameter requires an **exact datasource name**. Wildcards are not supported:

```
VALID:   p_ds = 2LIS_11_VAHDR
INVALID: p_ds = 2LIS_*
INVALID: p_ds = *
```

## Components

### Database Tables

| Table | Description |
|-------|-------------|
| ZBQTR_CONFIG | Auto-populated datasource configuration |
| ZBQTR_LOG | Replication audit log |
| ZBQTR_SUBSC | ODP subscription tracking |

### Classes

| Class | Description |
|-------|-------------|
| ZCX_BQ_REPLICATION_FAILED | Custom exception for failures |
| ZCL_BQ_REPLICATOR | BQ Toolkit wrapper with fallback logic |
| ZCL_BQ_ODP_SUBSCRIBER | ODP subscription manager |
| ZCL_IM_SAPI_BQ | BAdI implementation with subscriber verification |

### Programs

| Program | Description |
|---------|-------------|
| Z_BQ_EXTRACTOR_RUN | Main scheduler program |
| Z_BQ_EXTRACTOR_MONITOR | Monitoring and status report |

## Program Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| p_ds | Yes | Exact datasource name (no wildcards) |
| p_mtkey | Yes | BQ Toolkit Mass Transfer Key |
| p_struct | No | DDIC structure name (derived if blank) |
| p_mode | No | Extraction mode: D, F, I, R, A (default: D) |
| p_test | No | Test mode - no BQ write |
| p_conn | No | Pre-check BQ connection |

## Extraction Modes

| Mode | Description |
|------|-------------|
| D | Delta extraction (default) |
| F | Full extraction |
| I | Initialize subscription + register config |
| R | Recovery (reset + full) |
| A | Auto (Init+Full if new, Delta if exists) |

## Usage

### Initialize Datasource

```
Program: Z_BQ_EXTRACTOR_RUN
Parameters:
  p_ds     = 2LIS_02_SGR
  p_mtkey  = ZMTR_2LIS_02_SGR
  p_mode   = I
```

This creates the ODP subscription and auto-populates ZBQTR_CONFIG.

### Run Full Load

```
Parameters:
  p_ds     = 2LIS_02_SGR
  p_mtkey  = ZMTR_2LIS_02_SGR
  p_mode   = F
```

### Run Delta Extraction

```
Parameters:
  p_ds     = 2LIS_02_SGR
  p_mtkey  = ZMTR_2LIS_02_SGR
  p_mode   = D
```

### Monitor Status

```
Program: Z_BQ_EXTRACTOR_MONITOR
```

## Supported Datasources

### Logistics (2LIS_*)

- 2LIS_02_SCL - Schedule Line
- 2LIS_02_SGR - Goods Receipt
- 2LIS_02_ITM - Purchase Order Item
- 2LIS_11_VAHDR - Sales Order Header
- 2LIS_11_VAITM - Sales Order Item
- 2LIS_12_VCHDR - Delivery Header
- 2LIS_03_BF - Goods Movements
- And more...

### FI/CO (0FI_*/0CO_*)

- 0FI_GL_4 - GL Line Items
- 0FI_AR_4 - AR Line Items
- 0FI_AP_4 - AP Line Items
- 0CO_OM_CCA_1 - Cost Center Actuals
- And more...

## Error Handling

The solution implements fail-safe extraction:

1. If BigQuery API fails, exception is raised
2. ODP delta is NOT confirmed
3. Delta remains in ODQMON for automatic retry
4. Consecutive failures are tracked and alerted

## Documentation

- [Installation Guide](docs/INSTALL.md)
- [User Guide](docs/USER_GUIDE.md)
- [Technical Reference](docs/TECHNICAL.md)

## Project Structure

```
bqtoolkitextractor/
├── .abapgit.xml
├── README.md
├── docs/
│   ├── INSTALL.md
│   ├── USER_GUIDE.md
│   └── TECHNICAL.md
└── src/
    ├── package.devc.xml
    ├── zbqtr.msag.xml
    ├── zbqtr_config.tabl.xml
    ├── zbqtr_log.tabl.xml
    ├── zbqtr_subsc.tabl.xml
    ├── zcx_bq_replication_failed.clas.abap
    ├── zcx_bq_replication_failed.clas.xml
    ├── zcl_bq_replicator.clas.abap
    ├── zcl_bq_replicator.clas.xml
    ├── zcl_bq_odp_subscriber.clas.abap
    ├── zcl_bq_odp_subscriber.clas.xml
    ├── zcl_im_sapi_bq.clas.abap
    ├── zcl_im_sapi_bq.clas.xml
    ├── z_bqtr_sapi.sxci.xml
    ├── z_bq_extractor_run.prog.abap
    ├── z_bq_extractor_run.prog.xml
    ├── z_bq_extractor_monitor.prog.abap
    └── z_bq_extractor_monitor.prog.xml
```

## References

- [RSU5_SAPI_BADI](https://community.sap.com/t5/technology-blog-posts-by-members/implementing-extraction-enhancement-using-sapi-badi-encapsulation-via/ba-p/13205734)
- [BQ Toolkit for SAP](https://cloud.google.com/solutions/sap/docs/abap-sdk/on-premises-or-any-cloud/latest/bq-toolkit-for-sap-overview)
- [ODP Delta Queue Anatomy](https://nttdata-solutions.com/uk/blog/anatomy-of-the-operational-delta-queues-in-sap-odp-extractors/)

## License

Internal use only - SAP BQ Toolkit Integration
