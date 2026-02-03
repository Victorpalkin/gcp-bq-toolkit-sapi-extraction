CLASS zcl_bq_replicator DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

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
      RAISING
        zcx_bq_replication_failed.

    METHODS replicate
      IMPORTING
        it_data         TYPE STANDARD TABLE
        iv_updmode      TYPE char1
        iv_request_id   TYPE char32 OPTIONAL
      RETURNING
        VALUE(rs_result) TYPE ty_result
      RAISING
        zcx_bq_replication_failed.

    METHODS test_connection
      RETURNING VALUE(rv_connected) TYPE abap_bool.

    METHODS get_config
      RETURNING VALUE(rs_config) TYPE zbqtr_config.

  PRIVATE SECTION.
    DATA ms_config TYPE zbqtr_config.
    DATA mv_datasource TYPE char30.
    DATA mo_bq_load TYPE REF TO object.

    METHODS log_operation
      IMPORTING
        is_result   TYPE ty_result
        iv_updmode  TYPE char1
        iv_records  TYPE i.

    TYPES ty_log_id TYPE n LENGTH 20.

    METHODS generate_log_id
      RETURNING VALUE(rv_log_id) TYPE ty_log_id.

    METHODS send_in_batches
      IMPORTING
        it_data         TYPE STANDARD TABLE
        iv_request_id   TYPE char32
      RETURNING
        VALUE(rs_result) TYPE ty_result
      RAISING
        zcx_bq_replication_failed.

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

    " Try to get config from table (populated during init)
    SELECT SINGLE * FROM zbqtr_config
      WHERE datasource = @iv_datasource
      INTO @ms_config.

    IF sy-subrc <> 0.
      " Not in config - use provided parameters (first-time init or direct call)
      IF iv_mass_tr_key IS INITIAL.
        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = iv_datasource
            iv_error_text = 'Mass transfer key required (datasource not found in ZBQTR_CONFIG)'.
      ENDIF.
      ms_config-datasource = iv_datasource.
      ms_config-mass_tr_key = iv_mass_tr_key.
      ms_config-struct_name = iv_struct_name.
    ELSE.
      " Config found - override with provided parameters if supplied
      IF iv_mass_tr_key IS NOT INITIAL.
        ms_config-mass_tr_key = iv_mass_tr_key.
      ENDIF.
      IF iv_struct_name IS NOT INITIAL.
        ms_config-struct_name = iv_struct_name.
      ENDIF.
    ENDIF.

    IF ms_config-mass_tr_key IS INITIAL.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = iv_datasource
          iv_error_text = 'Mass transfer key not configured'.
    ENDIF.

    TRY.
        CREATE OBJECT mo_bq_load TYPE ('/GOOG/CL_BQTR_DATA_LOAD')
          EXPORTING
            iv_mass_tr_key = ms_config-mass_tr_key
            iv_struct_name = ms_config-struct_name.
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

    IF it_data IS INITIAL.
      rs_result-success = abap_true.
      rs_result-records_sent = 0.
      RETURN.
    ENDIF.

    GET TIME STAMP FIELD lv_start_time.

    rs_result-request_id = iv_request_id.

    TRY.
        IF ms_config-batch_size > 0 AND lines( it_data ) > ms_config-batch_size.
          rs_result = send_in_batches(
            it_data       = it_data
            iv_request_id = iv_request_id ).
        ELSE.
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

    rs_result-duration_ms = cl_abap_tstmp=>subtract(
      tstmp1 = lv_end_time
      tstmp2 = lv_start_time ) * 1000.

    log_operation(
      is_result  = rs_result
      iv_updmode = iv_updmode
      iv_records = lines( it_data ) ).

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

    CREATE DATA lt_batch LIKE it_data.
    ASSIGN lt_batch->* TO <lt_batch>.

    rs_result-success = abap_true.
    rs_result-request_id = iv_request_id.

    WHILE lv_from <= lv_total.
      lv_batch_num = lv_batch_num + 1.
      lv_to = lv_from + ms_config-batch_size - 1.
      IF lv_to > lv_total.
        lv_to = lv_total.
      ENDIF.

      CLEAR <lt_batch>.
      LOOP AT <lt_data> ASSIGNING FIELD-SYMBOL(<ls_row>) FROM lv_from TO lv_to.
        APPEND <ls_row> TO <lt_batch>.
      ENDLOOP.

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
      ls_log-status = 'S'.
    ELSE.
      ls_log-status = 'E'.
      ls_log-error_message = is_result-error_message.
    ENDIF.

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

    CALL FUNCTION 'GENERAL_GET_RANDOM_INT'
      EXPORTING
        range  = 999999
      IMPORTING
        random = lv_random.

    rv_log_id = |{ lv_timestamp TIMESTAMP = ISO }{ lv_random }|.
    REPLACE ALL OCCURRENCES OF PCRE '[^0-9]' IN rv_log_id WITH ''.
    rv_log_id = rv_log_id(20).
  ENDMETHOD.


  METHOD test_connection.
    TRY.
        CALL METHOD mo_bq_load->('CHECK_CONNECTION').
        rv_connected = abap_true.
      CATCH cx_sy_dyn_call_error.
        TRY.
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
