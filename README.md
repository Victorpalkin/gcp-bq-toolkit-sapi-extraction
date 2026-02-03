# SAP S-API to BigQuery Extraction Solution

A custom SAP extraction solution that replicates data from S-API extractors (Logistics 2LIS_* and FI/CO 0FI_*/0CO_*) to Google BigQuery using the BQ Toolkit.

## Features

- **Direct ODP Integration**: No SAP BW required - uses ODP Replication API directly
- **Scheduled Delta Extraction**: SM36 background job every 15 minutes
- **Fail-Safe Design**: Failed extractions leave deltas unconfirmed for automatic retry
- **Comprehensive Logging**: All operations tracked in ZBQTR_LOG
- **ODQMON Compatible**: Full integration with SAP's delta queue monitoring

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SM36 Job (Every 15 min)                                        │
│       ↓                                                         │
│  Z_BQ_EXTRACTOR_RUN → ZCL_BQ_ODP_SUBSCRIBER                    │
│       ↓                                                         │
│  ODP Framework → S-API Extractor → RSU5_SAPI_BADI              │
│       ↓                                                         │
│  ZCL_IM_SAPI_BQ → ZCL_BQ_REPLICATOR → BQ Toolkit               │
│       ↓                                                         │
│  Google BigQuery                                                │
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

### Installation Options

| Method | Best For |
|--------|----------|
| **abapGit** | Version control, easy updates, development systems |
| **Transport Request** | Production systems, traditional SAP deployment |

### Quick Start (abapGit)

1. Install via abapGit: Clone repository into package `ZBQTR`
2. Configure `/GOOG/BQTR_SETTINGS` for authentication
3. Add datasources to `ZBQTR_CONFIG` (SM30)
4. Initialize subscriptions: `Z_BQ_EXTRACTOR_RUN` with mode = I
5. Run full load: `Z_BQ_EXTRACTOR_RUN` with mode = F
6. Schedule delta job in SM36 (every 15 min)

### Quick Start (Transport)

1. Import transport request via STMS
2. Configure `/GOOG/BQTR_SETTINGS` for authentication
3. Add datasources to `ZBQTR_CONFIG` (SM30)
4. Initialize subscriptions: `Z_BQ_EXTRACTOR_RUN` with mode = I
5. Run full load: `Z_BQ_EXTRACTOR_RUN` with mode = F
6. Schedule delta job in SM36 (every 15 min)

## Components

### Database Tables

| Table | Description |
|-------|-------------|
| ZBQTR_CONFIG | Datasource configuration |
| ZBQTR_LOG | Replication audit log |
| ZBQTR_SUBSCRIPTION | ODP subscription tracking |

### Classes

| Class | Description |
|-------|-------------|
| ZCX_BQ_REPLICATION_FAILED | Custom exception for failures |
| ZCL_BQ_REPLICATOR | BQ Toolkit wrapper with error handling |
| ZCL_BQ_ODP_SUBSCRIBER | ODP subscription manager |
| ZCL_IM_SAPI_BQ | BAdI implementation |

### Programs

| Program | Description |
|---------|-------------|
| Z_BQ_EXTRACTOR_RUN | Main scheduler program |
| Z_BQ_EXTRACTOR_MONITOR | Monitoring and status report |

### BAdI

| Object | Description |
|--------|-------------|
| Z_SAPI_BQ_REPLICATION | Implementation of RSU5_SAPI_BADI |

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

## Usage

### Run Delta Extraction

```
Program: Z_BQ_EXTRACTOR_RUN
Parameters:
  p_ds = *           (all datasources)
  p_mode = D         (delta)
```

### Run Full Load

```
Program: Z_BQ_EXTRACTOR_RUN
Parameters:
  p_ds = 2LIS_02_SGR (specific datasource)
  p_mode = F         (full)
```

### Monitor Status

```
Program: Z_BQ_EXTRACTOR_MONITOR
```

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
├── README.md
├── docs/
│   ├── INSTALL.md
│   ├── USER_GUIDE.md
│   └── TECHNICAL.md
└── src/
    ├── tables/
    │   ├── zbqtr_config.tabl.xml
    │   ├── zbqtr_log.tabl.xml
    │   └── zbqtr_subscription.tabl.xml
    ├── classes/
    │   ├── zcx_bq_replication_failed.clas.abap
    │   ├── zcl_bq_replicator.clas.abap
    │   └── zcl_bq_odp_subscriber.clas.abap
    ├── badi/
    │   ├── zcl_im_sapi_bq.clas.abap
    │   └── z_sapi_bq_replication.enho.xml
    ├── programs/
    │   ├── z_bq_extractor_run.prog.abap
    │   └── z_bq_extractor_monitor.prog.abap
    └── zbqtr.msag.xml
```

## References

- [RSU5_SAPI_BADI](https://community.sap.com/t5/technology-blog-posts-by-members/implementing-extraction-enhancement-using-sapi-badi-encapsulation-via/ba-p/13205734)
- [BQ Toolkit for SAP](https://cloud.google.com/solutions/sap/docs/abap-sdk/on-premises-or-any-cloud/latest/bq-toolkit-for-sap-overview)
- [ODP Delta Queue Anatomy](https://nttdata-solutions.com/uk/blog/anatomy-of-the-operational-delta-queues-in-sap-odp-extractors/)

## License

Internal use only - SAP BQ Toolkit Integration
