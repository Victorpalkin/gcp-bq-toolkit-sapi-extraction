*&---------------------------------------------------------------------*
*& Report Z_BQ_EXTRACTOR_RUN
*&---------------------------------------------------------------------*
*& Purpose: Trigger S-API extraction to BigQuery
*&          Designed for SM36 scheduling every 15 minutes
*&
*& Usage:
*&   - Schedule via SM36 with variant for delta extraction
*&   - Run in dialog mode for testing and full loads
*&
*& Modes:
*&   D = Delta extraction (default, for scheduled runs)
*&   F = Full extraction (initial load or refresh)
*&   I = Initialize subscription (first time setup)
*&   R = Recovery/repeat delta
*&   A = Auto (Init+Full if new, Delta if exists)
*&
*& Author: SAP BQ Toolkit Integration
*&---------------------------------------------------------------------*
REPORT z_bq_extractor_run.

*----------------------------------------------------------------------*
* Selection Screen
*----------------------------------------------------------------------*
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.

PARAMETERS: p_ds     TYPE char30 OBLIGATORY,   " Datasource name (exact, no wildcards)
            p_mtkey  TYPE char20 OBLIGATORY,   " BQ Toolkit Mass Transfer Key
            p_struct TYPE char30,              " Structure name (optional, derived if blank)
            p_mode   TYPE char1 DEFAULT 'D'.   " D=Delta, F=Full, I=Init, R=Recovery, A=Auto

SELECTION-SCREEN SKIP.

PARAMETERS: p_test TYPE char1 AS CHECKBOX DEFAULT ' ',  " Test mode (no BQ write)
            p_conn TYPE char1 AS CHECKBOX DEFAULT ' '.  " Pre-check BQ connection

SELECTION-SCREEN END OF BLOCK b1.


*----------------------------------------------------------------------*
* Text Symbols
*----------------------------------------------------------------------*
* TEXT-001: Extraction Settings
* TEXT-010: Starting BQ Extractor Run
* TEXT-011: Processing datasource:
* TEXT-012: Records extracted:
* TEXT-013: Error:
* TEXT-014: Success
* TEXT-015: FAILED (will retry next run)
* TEXT-016: No active datasources found
* TEXT-017: Connection pre-check failed - skipping extraction
* TEXT-018: Extraction completed
* TEXT-019: Test mode enabled - no data sent to BigQuery


*----------------------------------------------------------------------*
* Local Class Definition
*----------------------------------------------------------------------*
CLASS lcl_extractor DEFINITION.
  PUBLIC SECTION.
    TYPES: BEGIN OF ty_result,
             datasource TYPE char30,
             mode       TYPE char1,
             records    TYPE i,
             success    TYPE abap_bool,
             message    TYPE string,
             duration   TYPE i,
           END OF ty_result,
           tt_result TYPE STANDARD TABLE OF ty_result WITH DEFAULT KEY.

    METHODS constructor
      IMPORTING
        iv_test_mode   TYPE abap_bool DEFAULT abap_false
        iv_mass_tr_key TYPE char20
        iv_struct_name TYPE char30 OPTIONAL.

    "! Run extraction for a single datasource
    METHODS run
      IMPORTING
        iv_datasource       TYPE char30
        iv_mode             TYPE char1
        iv_check_connection TYPE abap_bool DEFAULT abap_false
      RETURNING
        VALUE(rt_results) TYPE tt_result.

    "! Process a single datasource
    METHODS process_datasource
      IMPORTING
        iv_datasource     TYPE char30
        iv_mode           TYPE char1
      RETURNING
        VALUE(rs_result) TYPE ty_result.

  PRIVATE SECTION.
    DATA mv_test_mode TYPE abap_bool.
    DATA mv_mass_tr_key TYPE char20.
    DATA mv_struct_name TYPE char30.
    DATA mv_start_timestamp TYPE timestampl.

    "! Check BigQuery connectivity before extraction
    METHODS check_bq_connection
      IMPORTING
        iv_datasource TYPE char30
      RETURNING
        VALUE(rv_connected) TYPE abap_bool.

    "! Output result to list
    METHODS output_result
      IMPORTING
        is_result TYPE ty_result.

    "! Auto-populate ZBQTR_CONFIG during initialization
    METHODS register_datasource_config
      IMPORTING
        iv_datasource TYPE char30.

    "! Display replication log entries from ZBQTR_LOG
    METHODS display_log_entries
      IMPORTING
        iv_datasource TYPE char30.

ENDCLASS.


CLASS lcl_extractor IMPLEMENTATION.

  METHOD constructor.
    mv_test_mode = iv_test_mode.
    mv_mass_tr_key = iv_mass_tr_key.
    mv_struct_name = iv_struct_name.
  ENDMETHOD.


  METHOD run.
    GET TIME STAMP FIELD mv_start_timestamp.

    WRITE: / TEXT-010.
    WRITE: / |Datasource: { iv_datasource } - Mode: { iv_mode }|.
    ULINE.

    IF mv_test_mode = abap_true.
      WRITE: / TEXT-019.
    ENDIF.

    " Optional connection pre-check
    IF iv_check_connection = abap_true.
      IF check_bq_connection( iv_datasource ) = abap_false.
        WRITE: / TEXT-017, iv_datasource.
        APPEND VALUE #(
          datasource = iv_datasource
          mode       = iv_mode
          success    = abap_false
          message    = 'Connection pre-check failed'
        ) TO rt_results.
        RETURN.
      ENDIF.
    ENDIF.

    " Process datasource
    DATA(ls_result) = process_datasource(
      iv_datasource = iv_datasource
      iv_mode       = iv_mode ).

    APPEND ls_result TO rt_results.
    output_result( ls_result ).

    ULINE.
    WRITE: / TEXT-018.

    " Summary
    IF ls_result-success = abap_true.
      WRITE: / |Success: { ls_result-records } records extracted|.
    ELSE.
      WRITE: / |Failed: { ls_result-message }|.
    ENDIF.

    " Display log entries from BAdI
    display_log_entries( iv_datasource ).
  ENDMETHOD.


  METHOD process_datasource.
    DATA: lv_start TYPE timestampl,
          lv_end   TYPE timestampl.

    GET TIME STAMP FIELD lv_start.

    rs_result-datasource = iv_datasource.
    rs_result-mode = iv_mode.

    TRY.
        DATA(lo_subscriber) = NEW zcl_bq_odp_subscriber(
          iv_datasource  = iv_datasource
          iv_mass_tr_key = mv_mass_tr_key
          iv_struct_name = mv_struct_name ).

        CASE iv_mode.
          WHEN 'F'.  " Full load
            rs_result-records = lo_subscriber->run_full( ).
            rs_result-success = abap_true.
            rs_result-message = 'Full load completed'.

          WHEN 'D'.  " Delta load
            rs_result-records = lo_subscriber->run_delta( ).
            rs_result-success = abap_true.
            rs_result-message = 'Delta load completed'.

          WHEN 'I'.  " Initialize subscription
            lo_subscriber->initialize_subscription( ).
            " Auto-register in ZBQTR_CONFIG
            register_datasource_config( iv_datasource ).
            rs_result-records = 0.
            rs_result-success = abap_true.
            rs_result-message = 'Subscription initialized and config registered'.

          WHEN 'R'.  " Recovery/repeat
            " Reset and run full
            lo_subscriber->reset_subscription( ).
            rs_result-records = lo_subscriber->run_full( ).
            rs_result-success = abap_true.
            rs_result-message = 'Recovery completed'.

          WHEN 'A'.  " Auto mode
            DATA(lv_is_new) = COND abap_bool(
              WHEN lo_subscriber->subscription_exists( ) = abap_false
              THEN abap_true ELSE abap_false ).

            IF lv_is_new = abap_true.
              " Auto-register in ZBQTR_CONFIG for new datasources
              register_datasource_config( iv_datasource ).
            ENDIF.

            rs_result-records = lo_subscriber->run_auto( ).
            rs_result-success = abap_true.
            rs_result-message = COND #(
              WHEN lv_is_new = abap_true
              THEN 'Auto: Init + Full completed'
              ELSE 'Auto: Delta completed' ).

          WHEN OTHERS.
            rs_result-success = abap_false.
            rs_result-message = |Invalid mode: { iv_mode }|.
        ENDCASE.

      CATCH zcx_bq_replication_failed INTO DATA(lx_bq_error).
        rs_result-success = abap_false.
        rs_result-message = lx_bq_error->get_error_message( ).

      CATCH cx_root INTO DATA(lx_root).
        rs_result-success = abap_false.
        rs_result-message = lx_root->get_text( ).
    ENDTRY.

    GET TIME STAMP FIELD lv_end.
    rs_result-duration = cl_abap_tstmp=>subtract(
      tstmp1 = lv_end
      tstmp2 = lv_start ) * 1000.
  ENDMETHOD.


  METHOD check_bq_connection.
    TRY.
        DATA(lo_replicator) = NEW zcl_bq_replicator(
          iv_datasource  = iv_datasource
          iv_mass_tr_key = mv_mass_tr_key
          iv_struct_name = mv_struct_name ).
        rv_connected = lo_replicator->test_connection( ).
      CATCH zcx_bq_replication_failed.
        rv_connected = abap_false.
    ENDTRY.
  ENDMETHOD.


  METHOD register_datasource_config.
    DATA: ls_config TYPE zbqtr_config.

    ls_config-datasource       = iv_datasource.
    ls_config-mass_tr_key      = mv_mass_tr_key.
    ls_config-struct_name      = mv_struct_name.
    ls_config-subscriber_type  = zcl_bq_odp_subscriber=>c_subscriber_type.
    ls_config-subscriber_name  = zcl_bq_odp_subscriber=>c_subscriber_name.
    ls_config-init_date        = sy-datum.
    ls_config-init_time        = sy-uzeit.
    ls_config-init_by          = sy-uname.

    MODIFY zbqtr_config FROM ls_config.
    COMMIT WORK AND WAIT.
  ENDMETHOD.


  METHOD display_log_entries.
    DATA: lt_log TYPE STANDARD TABLE OF zbqtr_log.

    " Get log entries for this datasource from current run
    SELECT * FROM zbqtr_log
      WHERE datasource = @iv_datasource
        AND timestamp >= @mv_start_timestamp
      ORDER BY timestamp DESCENDING
      INTO TABLE @lt_log.

    IF lt_log IS NOT INITIAL.
      ULINE.
      WRITE: / 'Replication Log (from BAdI):'.
      WRITE: / '----------------------------'.

      DATA(lv_total_records) = 0.

      LOOP AT lt_log INTO DATA(ls_log).
        WRITE: / 'Timestamp:', ls_log-timestamp(14),
                 'Records:', ls_log-records_replicated,
                 'Status:', ls_log-status.
        IF ls_log-error_message IS NOT INITIAL.
          WRITE: / '  Error:', ls_log-error_message.
        ENDIF.
        lv_total_records = lv_total_records + ls_log-records_replicated.
      ENDLOOP.

      ULINE.
      WRITE: / 'Total records replicated:', lv_total_records.
    ELSE.
      WRITE: / 'No log entries found (BAdI may not have been triggered)'.
    ENDIF.
  ENDMETHOD.


  METHOD output_result.
    WRITE: / TEXT-011, is_result-datasource.

    IF is_result-success = abap_true.
      WRITE: / '  ->', TEXT-014.
      WRITE: / '  ', TEXT-012, is_result-records.
    ELSE.
      WRITE: / '  ->', TEXT-015.
      WRITE: / '  ', TEXT-013, is_result-message.
    ENDIF.

    WRITE: / |  Duration: { is_result-duration } ms|.
  ENDMETHOD.

ENDCLASS.


*----------------------------------------------------------------------*
* Main Program
*----------------------------------------------------------------------*
START-OF-SELECTION.

  " Validate no wildcards in datasource name
  IF p_ds CA '*%'.
    MESSAGE e000(zbqtr) WITH 'Wildcards not allowed. Specify exact datasource name.'.
    RETURN.
  ENDIF.

  " Validate mode parameter
  IF NOT p_mode CA 'FDIRA'.
    MESSAGE e000(zbqtr) WITH 'Invalid mode. Use F, D, I, R, or A.'.
    RETURN.
  ENDIF.

  " Create extractor and run
  DATA(lo_extractor) = NEW lcl_extractor(
    iv_test_mode   = COND #( WHEN p_test = 'X' THEN abap_true ELSE abap_false )
    iv_mass_tr_key = p_mtkey
    iv_struct_name = p_struct ).

  DATA(lt_results) = lo_extractor->run(
    iv_datasource       = p_ds
    iv_mode             = p_mode
    iv_check_connection = COND #( WHEN p_conn = 'X' THEN abap_true ELSE abap_false ) ).

  " Set return code based on results
  DATA(lv_failed) = REDUCE i( INIT f = 0 FOR r IN lt_results WHERE ( success = abap_false ) NEXT f = f + 1 ).

  IF lv_failed > 0.
    MESSAGE e000(zbqtr) WITH lv_failed 'datasource(s) failed'.
  ENDIF.
