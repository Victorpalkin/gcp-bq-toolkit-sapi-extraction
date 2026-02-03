"! <p class="shorttext synchronized">BQ Toolkit Wrapper for Data Replication</p>
"! Wraps the BQ Toolkit /GOOG/CL_BQTR_DATA_LOAD class to provide
"! error handling, logging, and fail-safe replication.
CLASS zcl_bq_replicator DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    "! Result structure for replication operations
    TYPES: BEGIN OF ty_result,
             success          TYPE abap_bool,
             error_code       TYPE i,
             error_message    TYPE string,
             records_sent     TYPE i,
             records_failed   TYPE i,
             duration_ms      TYPE i,
             request_id       TYPE char32,
           END OF ty_result.

    "! <p class="shorttext synchronized">Constructor</p>
    "!
    "! @parameter iv_datasource | DataSource name (must exist in ZBQTR_CONFIG)
    "! @raising zcx_bq_replication_failed | If configuration not found
    METHODS constructor
      IMPORTING
        iv_datasource TYPE char30
      RAISING
        zcx_bq_replication_failed.

    "! <p class="shorttext synchronized">Replicate data to BigQuery</p>
    "!
    "! @parameter it_data | Data table to replicate
    "! @parameter iv_updmode | Update mode (F=Full, D=Delta, R=Repeat, I=Init)
    "! @parameter iv_request_id | ODP request ID for tracking
    "! @parameter rs_result | Replication result
    "! @raising zcx_bq_replication_failed | If replication fails
    METHODS replicate
      IMPORTING
        it_data         TYPE STANDARD TABLE
        iv_updmode      TYPE char1
        iv_request_id   TYPE char32 OPTIONAL
      RETURNING
        VALUE(rs_result) TYPE ty_result
      RAISING
        zcx_bq_replication_failed.

    "! <p class="shorttext synchronized">Test BigQuery connectivity</p>
    "!
    "! @parameter rv_connected | True if connection successful
    METHODS test_connection
      RETURNING VALUE(rv_connected) TYPE abap_bool.

    "! <p class="shorttext synchronized">Get configuration for this replicator</p>
    "!
    "! @parameter rs_config | Configuration record
    METHODS get_config
      RETURNING VALUE(rs_config) TYPE zbqtr_config.

  PRIVATE SECTION.
    DATA ms_config TYPE zbqtr_config.
    DATA mv_datasource TYPE char30.
    DATA mo_bq_load TYPE REF TO object.  " /goog/cl_bqtr_data_load

    "! Log replication operation to ZBQTR_LOG
    METHODS log_operation
      IMPORTING
        is_result   TYPE ty_result
        iv_updmode  TYPE char1
        iv_records  TYPE i.

    "! Generate unique log ID
    METHODS generate_log_id
      RETURNING VALUE(rv_log_id) TYPE numc20.

    "! Send data in batches respecting batch_size config
    METHODS send_in_batches
      IMPORTING
        it_data         TYPE STANDARD TABLE
        iv_request_id   TYPE char32
      RETURNING
        VALUE(rs_result) TYPE ty_result
      RAISING
        zcx_bq_replication_failed.

    "! Call BQ Toolkit REPLICATE_DATA method
    METHODS call_bq_toolkit
      IMPORTING
        it_data TYPE STANDARD TABLE
      EXPORTING
        ev_error_code TYPE i
        et_return TYPE bapiret2_t
      RAISING
        zcx_bq_replication_failed.

ENDCLASS.


CLASS zcl_bq_replicator IMPLEMENTATION.

  METHOD constructor.
    mv_datasource = iv_datasource.

    " Load configuration from ZBQTR_CONFIG
    SELECT SINGLE * FROM zbqtr_config
      WHERE datasource = @iv_datasource
      INTO @ms_config.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = iv_datasource
          iv_error_text = |Configuration not found for datasource { iv_datasource }|
          textid        = zcx_bq_replication_failed=>config_not_found.
    ENDIF.

    " Validate required configuration fields
    IF ms_config-mass_tr_key IS INITIAL.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = iv_datasource
          iv_error_text = 'Mass transfer key not configured'.
    ENDIF.

    " Create BQ Toolkit instance
    TRY.
        CREATE OBJECT mo_bq_load TYPE ('/GOOG/CL_BQTR_DATA_LOAD')
          EXPORTING
            iv_mass_tr_key = CONV char20( ms_config-mass_tr_key )
            iv_struct_name = CONV char30( ms_config-struct_name ).
      CATCH cx_sy_create_object_error INTO DATA(lx_create).
        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = iv_datasource
            iv_error_text = |Failed to create BQ Toolkit instance: { lx_create->get_text( ) }|
            previous      = lx_create.
    ENDTRY.
  ENDMETHOD.


  METHOD replicate.
    DATA: lv_start_time TYPE timestampl,
          lv_end_time   TYPE timestampl.

    " Skip empty data
    IF it_data IS INITIAL.
      rs_result-success = abap_true.
      rs_result-records_sent = 0.
      RETURN.
    ENDIF.

    GET TIME STAMP FIELD lv_start_time.

    rs_result-request_id = iv_request_id.

    TRY.
        " Send data in batches if batch_size is configured
        IF ms_config-batch_size > 0 AND lines( it_data ) > ms_config-batch_size.
          rs_result = send_in_batches(
            it_data       = it_data
            iv_request_id = iv_request_id ).
        ELSE.
          " Send all data at once
          DATA: lv_error_code TYPE i,
                lt_return     TYPE bapiret2_t.

          call_bq_toolkit(
            EXPORTING
              it_data       = it_data
            IMPORTING
              ev_error_code = lv_error_code
              et_return     = lt_return ).

          IF lv_error_code = 0.
            rs_result-success = abap_true.
            rs_result-records_sent = lines( it_data ).
          ELSE.
            rs_result-success = abap_false.
            rs_result-error_code = lv_error_code.
            " Get first error message
            LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
              rs_result-error_message = ls_return-message.
              EXIT.
            ENDLOOP.
          ENDIF.
        ENDIF.

      CATCH zcx_bq_replication_failed INTO DATA(lx_bq_error).
        rs_result-success = abap_false.
        rs_result-error_code = lx_bq_error->mv_error_code.
        rs_result-error_message = lx_bq_error->mv_error_text.
    ENDTRY.

    GET TIME STAMP FIELD lv_end_time.

    " Calculate duration in milliseconds
    rs_result-duration_ms = cl_abap_tstmp=>subtract(
      tstmp1 = lv_end_time
      tstmp2 = lv_start_time ) * 1000.

    " Log the operation
    log_operation(
      is_result  = rs_result
      iv_updmode = iv_updmode
      iv_records = lines( it_data ) ).

    " Raise exception if failed
    IF rs_result-success = abap_false.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_code = rs_result-error_code
          iv_error_text = rs_result-error_message
          iv_request_id = iv_request_id.
    ENDIF.
  ENDMETHOD.


  METHOD send_in_batches.
    DATA: lt_batch     TYPE REF TO data,
          lv_from      TYPE i VALUE 1,
          lv_to        TYPE i,
          lv_total     TYPE i,
          lv_batch_num TYPE i VALUE 0.

    FIELD-SYMBOLS: <lt_data>  TYPE STANDARD TABLE,
                   <lt_batch> TYPE STANDARD TABLE.

    ASSIGN it_data TO <lt_data>.
    lv_total = lines( <lt_data> ).

    " Create batch table with same structure
    CREATE DATA lt_batch LIKE it_data.
    ASSIGN lt_batch->* TO <lt_batch>.

    rs_result-success = abap_true.
    rs_result-request_id = iv_request_id.

    " Process in batches
    WHILE lv_from <= lv_total.
      lv_batch_num = lv_batch_num + 1.
      lv_to = lv_from + ms_config-batch_size - 1.
      IF lv_to > lv_total.
        lv_to = lv_total.
      ENDIF.

      " Extract batch
      CLEAR <lt_batch>.
      LOOP AT <lt_data> ASSIGNING FIELD-SYMBOL(<ls_row>) FROM lv_from TO lv_to.
        APPEND <ls_row> TO <lt_batch>.
      ENDLOOP.

      " Send batch
      DATA: lv_error_code TYPE i,
            lt_return     TYPE bapiret2_t.

      TRY.
          call_bq_toolkit(
            EXPORTING
              it_data       = <lt_batch>
            IMPORTING
              ev_error_code = lv_error_code
              et_return     = lt_return ).

          IF lv_error_code = 0.
            rs_result-records_sent = rs_result-records_sent + lines( <lt_batch> ).
          ELSE.
            rs_result-success = abap_false.
            rs_result-error_code = lv_error_code.
            rs_result-records_failed = rs_result-records_failed + lines( <lt_batch> ).
            LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
              rs_result-error_message = ls_return-message.
              EXIT.
            ENDLOOP.
            " Stop on first batch failure
            EXIT.
          ENDIF.
        CATCH zcx_bq_replication_failed INTO DATA(lx_error).
          rs_result-success = abap_false.
          rs_result-error_code = lx_error->mv_error_code.
          rs_result-error_message = lx_error->mv_error_text.
          rs_result-records_failed = lv_total - rs_result-records_sent.
          EXIT.
      ENDTRY.

      lv_from = lv_to + 1.
    ENDWHILE.
  ENDMETHOD.


  METHOD call_bq_toolkit.
    " Call BQ Toolkit REPLICATE_DATA method via dynamic call
    " This avoids hard dependency on /GOOG/ namespace at compile time
    TRY.
        CALL METHOD mo_bq_load->('REPLICATE_DATA')
          EXPORTING
            it_data = it_data
          IMPORTING
            ev_error_code = ev_error_code
            et_return = et_return.
      CATCH cx_sy_dyn_call_error INTO DATA(lx_call).
        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = mv_datasource
            iv_error_text = |BQ Toolkit call failed: { lx_call->get_text( ) }|
            previous      = lx_call.
    ENDTRY.
  ENDMETHOD.


  METHOD log_operation.
    DATA: ls_log TYPE zbqtr_log.

    ls_log-log_id              = generate_log_id( ).
    GET TIME STAMP FIELD ls_log-timestamp.
    ls_log-datasource          = mv_datasource.
    ls_log-updmode             = iv_updmode.
    ls_log-records_extracted   = iv_records.
    ls_log-records_replicated  = is_result-records_sent.
    ls_log-duration_ms         = is_result-duration_ms.
    ls_log-request_id          = is_result-request_id.

    IF is_result-success = abap_true.
      ls_log-status = 'S'.  " Success
    ELSE.
      ls_log-status = 'E'.  " Error
      ls_log-error_message = is_result-error_message.
    ENDIF.

    " Capture job info if running in background
    IF sy-batch = abap_true.
      ls_log-job_name  = sy-repid.
      ls_log-job_count = sy-uzeit.
    ENDIF.

    INSERT zbqtr_log FROM ls_log.
    COMMIT WORK AND WAIT.
  ENDMETHOD.


  METHOD generate_log_id.
    DATA: lv_timestamp TYPE timestampl,
          lv_random    TYPE i.

    GET TIME STAMP FIELD lv_timestamp.

    " Generate random component for uniqueness
    CALL FUNCTION 'GENERAL_GET_RANDOM_INT'
      EXPORTING
        range  = 999999
      IMPORTING
        random = lv_random.

    " Combine timestamp and random for unique ID
    rv_log_id = |{ lv_timestamp TIMESTAMP = ISO }{ lv_random }|.
    REPLACE ALL OCCURRENCES OF REGEX '[^0-9]' IN rv_log_id WITH ''.
    rv_log_id = rv_log_id(20).
  ENDMETHOD.


  METHOD test_connection.
    " Test BigQuery connectivity by attempting a lightweight operation
    TRY.
        " Try to call CHECK_CONNECTION method if available
        CALL METHOD mo_bq_load->('CHECK_CONNECTION').
        rv_connected = abap_true.
      CATCH cx_sy_dyn_call_error.
        " Method doesn't exist - try alternate approach
        TRY.
            " Attempt to replicate empty table - should succeed quickly
            DATA: lt_empty TYPE STANDARD TABLE OF char1,
                  lv_error TYPE i,
                  lt_return TYPE bapiret2_t.

            call_bq_toolkit(
              EXPORTING
                it_data       = lt_empty
              IMPORTING
                ev_error_code = lv_error
                et_return     = lt_return ).

            rv_connected = COND #( WHEN lv_error = 0 THEN abap_true ELSE abap_false ).
          CATCH zcx_bq_replication_failed.
            rv_connected = abap_false.
        ENDTRY.
      CATCH cx_root.
        rv_connected = abap_false.
    ENDTRY.
  ENDMETHOD.


  METHOD get_config.
    rs_config = ms_config.
  ENDMETHOD.

ENDCLASS.
