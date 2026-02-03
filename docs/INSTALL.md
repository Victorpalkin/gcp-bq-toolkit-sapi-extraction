# Installation Guide

## SAP S-API to BigQuery Extraction Solution

This guide covers the installation and initial configuration of the BQ Toolkit Extractor solution.

---

## Prerequisites

### SAP System Requirements
- **SAP_BASIS** >= 7.40 SP05 (NetWeaver 7.40+)
- **SAP ECC** 6.0 EHP7/8 or S/4HANA
- ODP Framework enabled (standard with NW 7.40+)

### Google Cloud Requirements
- Google Cloud project with BigQuery API enabled
- Service account with the following roles:
  - `roles/bigquery.dataEditor` - Write data to BigQuery
  - `roles/bigquery.jobUser` - Run BigQuery jobs
- Service account key (JSON) or Workload Identity Federation configured

### ABAP SDK / BQ Toolkit Requirements
- **ABAP SDK for Google Cloud** (On-premises edition) v1.9+
- BQ Toolkit installed and configured via `/GOOG/BQTR_SETTINGS`

---

## Installation Options

Choose one of the following installation methods:

| Method | Best For | Requirements |
|--------|----------|--------------|
| **Option A: abapGit** | Version control, easy updates, development systems | abapGit installed |
| **Option B: Transport Request** | Production systems, traditional SAP deployment | STMS access |

---

## Option A: abapGit Installation (Recommended for Development)

abapGit provides version control integration and simplified updates from the repository.

### Prerequisites for abapGit

- abapGit installed in your SAP system
  - Transaction `ZABAPGIT` (standalone) or
  - SE80 abapGit plugin
- Developer authorization and developer key
- Target package created (recommendation: `ZBQTR`)

### Step A1: Prepare Target Package

1. Navigate to transaction **SE80** or **SE21**
2. Create a new package:
   - **Package**: `ZBQTR`
   - **Short Description**: `BQ Toolkit S-API Extractor`
   - **Software Component**: `HOME` or your local component
   - **Transport Layer**: Your development layer
3. Create and release the transport request for the package

### Step A2: Clone Repository via abapGit

#### Online Installation (if SAP system has internet access)

1. Open abapGit via transaction **ZABAPGIT**
2. Click **+ New Online**
3. Enter repository details:
   - **Repository URL**: `https://github.com/your-org/bqtoolkitextractor.git`
   - **Package**: `ZBQTR`
   - **Branch**: `main`
4. Click **Create Online Repo**
5. Click **Pull** to import all objects

#### Offline Installation (air-gapped systems)

1. Download the repository as a ZIP file from GitHub
2. Open abapGit via transaction **ZABAPGIT**
3. Click **+ New Offline**
4. Enter details:
   - **Repository Name**: `bqtoolkitextractor`
   - **Package**: `ZBQTR`
5. Click **Create Offline Repo**
6. Click **Import ZIP** and upload the downloaded file
7. Click **Pull** to import all objects

### Step A3: Activate Objects

1. After pulling, abapGit will show objects to activate
2. Click **Activate** to activate all objects
3. Resolve any activation errors in dependency order:
   - Tables first (ZBQTR_CONFIG, ZBQTR_LOG, ZBQTR_SUBSC)
   - Exception class (ZCX_BQ_REPLICATION_FAILED)
   - Classes (ZCL_BQ_REPLICATOR, ZCL_BQ_ODP_SUBSCRIBER, ZCL_IM_SAPI_BQ)
   - BAdI implementation
   - Programs last

### Step A4: Proceed to Configuration

After successful activation, continue with **Step 2: Configure Google Cloud Authentication** below.

### Updating via abapGit

To get updates from the repository:

1. Open abapGit transaction **ZABAPGIT**
2. Select the `bqtoolkitextractor` repository
3. Click **Pull** to fetch and merge changes
4. Activate changed objects
5. Transport to QA/Production as needed

---

## Option B: Transport Request Installation (Traditional)

Use this method for production deployments or systems without abapGit.

### Step B1: Import Transport Request

1. Download the transport files from the release package:
   - `K9xxxxx.DEV` (data file)
   - `R9xxxxx.DEV` (cofiles)
2. Copy files to your transport directory:
   - Data file: `/usr/sap/trans/data/`
   - Cofiles: `/usr/sap/trans/cofiles/`
3. Add transport to import queue in transaction **STMS**
4. Import into your development system
5. Transport contains:
   - Tables: `ZBQTR_CONFIG`, `ZBQTR_LOG`, `ZBQTR_SUBSC`
   - Classes: `ZCX_BQ_REPLICATION_FAILED`, `ZCL_BQ_REPLICATOR`, `ZCL_BQ_ODP_SUBSCRIBER`, `ZCL_IM_SAPI_BQ`
   - Programs: `Z_BQ_EXTRACTOR_RUN`, `Z_BQ_EXTRACTOR_MONITOR`
   - BAdI Implementation: `Z_SAPI_BQ_REPLICATION`
   - Message Class: `ZBQTR`

### Step B2: Proceed to Configuration

After successful import, continue with **Step 2: Configure Google Cloud Authentication** below.

---

## Configuration Steps (Both Installation Methods)

### Step 2: Configure Google Cloud Authentication

#### Option A: Service Account Key

1. Navigate to transaction `/GOOG/SLT_SETTINGS` or `/GOOG/BQTR_SETTINGS`
2. Create a new Client Key entry:
   - **Name**: `ZBQTR_CLIENT`
   - **Service Account Name**: Your GCP service account email
   - **Service Account Key**: Import from JSON file
3. Test connection using the provided test button

#### Option B: Workload Identity Federation (Recommended for Production)

1. Configure Workload Identity in Google Cloud Console
2. Set up the ABAP SDK to use WIF as per Google documentation
3. No key management required - uses SAP system identity

### Step 3: Configure Mass Transfer Keys in BQ Toolkit

For each datasource you want to replicate, create a Mass Transfer Key:

1. Navigate to transaction `/GOOG/BQTR_SETTINGS`
2. Create Mass Transfer Key entry:

| Field | Description | Example |
|-------|-------------|---------|
| Mass Transfer Key | Unique identifier | `ZMTR_2LIS_02_SGR` |
| Client Key | From Step 2 | `ZBQTR_CLIENT` |
| Target Dataset | BigQuery dataset | `sap_extracts` |
| Target Table | BigQuery table | `logistics_goods_receipts` |
| Structure Name | DDIC structure | `MC02M_0GRTP` |

**Note**: ZBQTR_CONFIG is auto-populated during initialization. You only need to configure Mass Transfer Keys in `/GOOG/BQTR_SETTINGS`.

### Step 4: Activate BAdI Implementation

1. Navigate to transaction **SE19**
2. Open BAdI Implementation `Z_SAPI_BQ_REPLICATION`
3. Verify it is set to **Active**
4. Check that the implementing class `ZCL_IM_SAPI_BQ` is correct

Alternatively, use transaction **SE18** to view the BAdI definition `RSU5_SAPI_BADI` and confirm the implementation is registered.

### Step 5: Initialize First Datasource

Initialize an ODP subscription and auto-register configuration:

1. Run program `Z_BQ_EXTRACTOR_RUN` in dialog mode (SE38)
2. Parameters:
   - **p_ds**: Exact datasource name (e.g., `2LIS_02_SGR`)
   - **p_mtkey**: Mass Transfer Key from Step 3 (e.g., `ZMTR_2LIS_02_SGR`)
   - **p_struct**: (optional) DDIC structure name
   - **p_mode**: `I` (Initialize)
3. Execute

This:
- Creates the ODP subscription in ODQMON
- Auto-populates ZBQTR_CONFIG with datasource configuration

### Step 6: Run Initial Full Load

After initialization, run a full load to populate BigQuery:

1. Run program `Z_BQ_EXTRACTOR_RUN`
2. Parameters:
   - **p_ds**: Same datasource name
   - **p_mtkey**: Same Mass Transfer Key
   - **p_mode**: `F` (Full)
3. Execute

**Note**: Full loads can take significant time depending on data volume.

### Step 7: Configure Background Job (SM36)

1. Navigate to transaction **SM36**
2. Create a new job:
   - **Job Name**: `Z_BQ_DELTA_<DATASOURCE>`
   - **Job Class**: B (Normal priority)
3. Define step:
   - **Program**: `Z_BQ_EXTRACTOR_RUN`
   - **Variant**: Create variant with:
     - p_ds = `<datasource name>`
     - p_mtkey = `<mass transfer key>`
     - p_mode = `D`
     - p_test = ` ` (unchecked)
     - p_conn = `X` (recommended for connection pre-check)
4. Define start condition:
   - **Period**: Every 15 minutes
   - Alternatively: Use calendar or custom schedule
5. Save and release the job

Repeat for each datasource you want to extract.

---

## Verification

### Test Single Extraction

```
1. Create test transaction in SAP (e.g., goods receipt via MIGO)
2. Run Z_BQ_EXTRACTOR_RUN with:
   - p_ds = <datasource>
   - p_mtkey = <mass transfer key>
   - p_mode = D
3. Check ZBQTR_LOG for success entry
4. Verify data in BigQuery Console
```

### Check ZBQTR_CONFIG

1. Transaction **SE16**
2. Table: `ZBQTR_CONFIG`
3. Verify datasource is registered with correct mass transfer key

### Check ODQMON

1. Transaction **ODQMON**
2. Select Subscriptions view
3. Filter by Subscriber = `ZBQTR_SUBSCRIBER`
4. Verify datasources show as "Active" with confirmed deltas

### Monitor Log

1. Run `Z_BQ_EXTRACTOR_MONITOR`
2. Select "Log entries" view
3. Check for successful extractions (green status)

---

## Troubleshooting

### BAdI Not Triggering

- Verify BAdI implementation is active (SE19)
- Check that datasource is in ZBQTR_CONFIG (run initialization first)
- Ensure subscriber_type and subscriber_name match in config

### BigQuery Connection Errors

- Check service account permissions in GCP Console
- Verify Client Key configuration in `/GOOG/BQTR_SETTINGS`
- Test connection using BQ Toolkit test utilities

### "Mass transfer key required" Error

- The p_mtkey parameter is mandatory
- Ensure mass transfer key exists in `/GOOG/BQTR_SETTINGS`

### "Wildcards not allowed" Error

- The p_ds parameter requires exact datasource name
- Wildcards (`*`, `%`) are not supported
- Specify the full datasource name (e.g., `2LIS_11_VAHDR`)

### Delta Not Confirmed

- Check ZBQTR_LOG for error messages
- The delta will automatically retry on next scheduled run
- If persistent, check BigQuery quotas and API limits

### ODP Subscription Issues

- Use ODQMON to view subscription status
- Reset subscription if needed via `Z_BQ_EXTRACTOR_RUN` with mode = R
- Check SAP Note 2232584 for ODP troubleshooting

### abapGit Pull Errors

- Ensure all objects are in the target package
- Check for naming conflicts with existing objects
- Verify user has developer key for all object types
- Try pulling in offline mode if online fails

---

## Migration from Previous Version

If upgrading from a version that used manual ZBQTR_CONFIG maintenance:

1. **Backup existing config**: Export ZBQTR_CONFIG entries
2. **Pull new code**: Update via abapGit or transport
3. **Re-initialize datasources**: For each existing datasource:
   ```
   p_ds = <datasource>
   p_mtkey = <mass transfer key from old config>
   p_mode = I
   ```
   This populates the new config table structure

4. **Update job variants**: Ensure p_mtkey is included in all variants
5. **Test**: Run delta extraction and verify data flow

---

## Next Steps

- Read [USER_GUIDE.md](USER_GUIDE.md) for operational procedures
- Read [TECHNICAL.md](TECHNICAL.md) for architecture details
- Configure alerting for consecutive failures
- Set up BigQuery monitoring dashboards
