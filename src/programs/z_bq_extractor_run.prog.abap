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
*&
*& Author: SAP BQ Toolkit Integration
*&---------------------------------------------------------------------*
REPORT z_bq_extractor_run.

*----------------------------------------------------------------------*
* Selection Screen
*----------------------------------------------------------------------*
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.

PARAMETERS: p_ds   TYPE char30 DEFAULT '*',  " Datasource filter (* = all)
            p_mode TYPE char1 DEFAULT 'D'.   " D=Delta, F=Full, I=Init, R=Recovery

SELECTION-SCREEN SKIP.

PARAMETERS: p_test TYPE char1 AS CHECKBOX DEFAULT ' ',  " Test mode (no BQ write)
            p_conn TYPE char1 AS CHECKBOX DEFAULT ' '.  " Pre-check BQ connection

SELECTION-SCREEN END OF BLOCK b1.

SELECTION-SCREEN BEGIN OF BLOCK b2 WITH FRAME TITLE TEXT-002.

PARAMETERS: p_para TYPE char1 AS CHECKBOX DEFAULT ' ',  " Parallel processing
            p_pgrp TYPE char10 DEFAULT 'ZBQTR'.         " Server group for parallel

SELECTION-SCREEN END OF BLOCK b2.


*----------------------------------------------------------------------*
* Text Symbols
*----------------------------------------------------------------------*
* TEXT-001: Extraction Settings
* TEXT-002: Performance Options
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
        iv_test_mode TYPE abap_bool DEFAULT abap_false.

    "! Run extraction for all matching datasources
    METHODS run
      IMPORTING
        iv_datasource_filter TYPE char30
        iv_mode              TYPE char1
        iv_check_connection  TYPE abap_bool DEFAULT abap_false
        iv_parallel          TYPE abap_bool DEFAULT abap_false
        iv_parallel_group    TYPE char10 DEFAULT 'ZBQTR'
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

ENDCLASS.


CLASS lcl_extractor IMPLEMENTATION.

  METHOD constructor.
    mv_test_mode = iv_test_mode.
  ENDMETHOD.


  METHOD run.
    DATA: lt_config TYPE TABLE OF zbqtr_config,
          lv_filter TYPE char30.

    " Convert * to % for LIKE
    lv_filter = iv_datasource_filter.
    REPLACE ALL OCCURRENCES OF '*' IN lv_filter WITH '%'.

    " Get active datasources matching filter
    SELECT * FROM zbqtr_config
      WHERE active = 'X'
        AND datasource LIKE @lv_filter
      ORDER BY datasource
      INTO TABLE @lt_config.

    IF lt_config IS INITIAL.
      WRITE: / TEXT-016, iv_datasource_filter.
      RETURN.
    ENDIF.

    WRITE: / TEXT-010.
    WRITE: / |Mode: { iv_mode } | Datasources: { lines( lt_config ) }|.
    ULINE.

    IF mv_test_mode = abap_true.
      WRITE: / TEXT-019.
    ENDIF.

    " Process each datasource
    LOOP AT lt_config INTO DATA(ls_config).

      " Skip full-only datasources in delta mode
      IF ls_config-full_only = abap_true AND iv_mode = 'D'.
        WRITE: / |Skipping { ls_config-datasource } (full-only)|.
        CONTINUE.
      ENDIF.

      " Optional connection pre-check
      IF iv_check_connection = abap_true.
        IF check_bq_connection( ls_config-datasource ) = abap_false.
          WRITE: / TEXT-017, ls_config-datasource.
          APPEND VALUE #(
            datasource = ls_config-datasource
            mode       = iv_mode
            success    = abap_false
            message    = 'Connection pre-check failed'
          ) TO rt_results.
          CONTINUE.
        ENDIF.
      ENDIF.

      " Process datasource
      DATA(ls_result) = process_datasource(
        iv_datasource = ls_config-datasource
        iv_mode       = iv_mode ).

      APPEND ls_result TO rt_results.
      output_result( ls_result ).
    ENDLOOP.

    ULINE.
    WRITE: / TEXT-018.

    " Summary
    DATA(lv_success) = REDUCE i( INIT s = 0 FOR r IN rt_results WHERE ( success = abap_true ) NEXT s = s + 1 ).
    DATA(lv_failed) = lines( rt_results ) - lv_success.
    DATA(lv_total_records) = REDUCE i( INIT t = 0 FOR r IN rt_results NEXT t = t + r-records ).

    WRITE: / |Summary: { lv_success } succeeded, { lv_failed } failed, { lv_total_records } total records|.
  ENDMETHOD.


  METHOD process_datasource.
    DATA: lv_start TYPE timestampl,
          lv_end   TYPE timestampl.

    GET TIME STAMP FIELD lv_start.

    rs_result-datasource = iv_datasource.
    rs_result-mode = iv_mode.

    TRY.
        DATA(lo_subscriber) = NEW zcl_bq_odp_subscriber( iv_datasource = iv_datasource ).

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
            rs_result-records = 0.
            rs_result-success = abap_true.
            rs_result-message = 'Subscription initialized'.

          WHEN 'R'.  " Recovery/repeat
            " Reset and run full
            lo_subscriber->reset_subscription( ).
            rs_result-records = lo_subscriber->run_full( ).
            rs_result-success = abap_true.
            rs_result-message = 'Recovery completed'.

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
        DATA(lo_replicator) = NEW zcl_bq_replicator( iv_datasource = iv_datasource ).
        rv_connected = lo_replicator->test_connection( ).
      CATCH zcx_bq_replication_failed.
        rv_connected = abap_false.
    ENDTRY.
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

  " Validate mode parameter
  IF NOT p_mode CA 'FDIR'.
    MESSAGE e000(zbqtr) WITH 'Invalid mode. Use F, D, I, or R.'.
    RETURN.
  ENDIF.

  " Create extractor and run
  DATA(lo_extractor) = NEW lcl_extractor(
    iv_test_mode = COND #( WHEN p_test = 'X' THEN abap_true ELSE abap_false ) ).

  DATA(lt_results) = lo_extractor->run(
    iv_datasource_filter = p_ds
    iv_mode              = p_mode
    iv_check_connection  = COND #( WHEN p_conn = 'X' THEN abap_true ELSE abap_false )
    iv_parallel          = COND #( WHEN p_para = 'X' THEN abap_true ELSE abap_false )
    iv_parallel_group    = p_pgrp ).

  " Set return code based on results
  DATA(lv_failed) = REDUCE i( INIT f = 0 FOR r IN lt_results WHERE ( success = abap_false ) NEXT f = f + 1 ).

  IF lv_failed > 0.
    MESSAGE e000(zbqtr) WITH lv_failed 'datasource(s) failed'.
  ENDIF.
