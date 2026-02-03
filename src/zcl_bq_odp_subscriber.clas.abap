CLASS zcl_bq_odp_subscriber DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    CONSTANTS c_subscriber_type TYPE char10 VALUE 'ZBQTR'.
    CONSTANTS c_subscriber_name TYPE char30 VALUE 'ZBQTR_SUBSCRIBER'.
    CONSTANTS c_context_sapi TYPE char10 VALUE 'SAPI'.

    CONSTANTS:
      c_mode_full     TYPE char1 VALUE 'F',
      c_mode_delta    TYPE char1 VALUE 'D',
      c_mode_init     TYPE char1 VALUE 'I',
      c_mode_recovery TYPE char1 VALUE 'R'.

    METHODS constructor
      IMPORTING
        iv_datasource TYPE char30.

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

    METHODS get_subscription_status
      RETURNING VALUE(rs_status) TYPE zbqtr_subsc.

    METHODS get_datasource
      RETURNING VALUE(rv_datasource) TYPE char30.

    METHODS get_pointer
      RETURNING VALUE(rv_pointer) TYPE char32.

  PRIVATE SECTION.
    DATA mv_datasource TYPE char30.
    DATA mv_pointer TYPE char32.
    DATA mv_process_id TYPE char30.
    DATA mv_current_mode TYPE char1.

    METHODS open_extraction
      IMPORTING
        iv_mode TYPE char1
      RAISING zcx_bq_replication_failed.

    METHODS fetch_and_process
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    METHODS close_extraction
      IMPORTING
        iv_confirmed TYPE abap_bool
      RAISING zcx_bq_replication_failed.

    METHODS update_subscription_tracking
      IMPORTING
        iv_status  TYPE char1
        iv_records TYPE i
        iv_error   TYPE string OPTIONAL.

    METHODS generate_process_id
      RETURNING VALUE(rv_id) TYPE char30.

    METHODS record_failure
      IMPORTING
        iv_error TYPE string.

    METHODS record_success
      IMPORTING
        iv_records TYPE i.

ENDCLASS.


CLASS zcl_bq_odp_subscriber IMPLEMENTATION.

  METHOD constructor.
    mv_datasource = iv_datasource.
    mv_process_id = generate_process_id( ).
  ENDMETHOD.


  METHOD generate_process_id.
    DATA: lv_random TYPE i.

    CALL FUNCTION 'GENERAL_GET_RANDOM_INT'
      EXPORTING
        range  = 9999
      IMPORTING
        random = lv_random.

    rv_id = |ZBQTR_{ sy-datum }_{ sy-uzeit }_{ lv_random }|.
  ENDMETHOD.


  METHOD initialize_subscription.
    TRY.
        open_extraction( c_mode_init ).
        close_extraction( abap_true ).

        DATA: ls_sub TYPE zbqtr_subsc.
        ls_sub-datasource     = mv_datasource.
        ls_sub-subscriber_id  = c_subscriber_name.
        ls_sub-init_date      = sy-datum.
        ls_sub-init_time      = sy-uzeit.
        ls_sub-status         = 'A'.

        MODIFY zbqtr_subsc FROM ls_sub.
        COMMIT WORK AND WAIT.

        rv_success = abap_true.

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        rv_success = abap_false.
        RAISE EXCEPTION lx_error.
    ENDTRY.
  ENDMETHOD.


  METHOD run_full.
    mv_current_mode = c_mode_full.

    TRY.
        open_extraction( c_mode_full ).
        rv_records = fetch_and_process( ).
        close_extraction( abap_true ).
        record_success( rv_records ).

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        TRY.
            close_extraction( abap_false ).
          CATCH cx_root.
        ENDTRY.
        record_failure( lx_error->mv_error_text ).
        RAISE EXCEPTION lx_error.
    ENDTRY.
  ENDMETHOD.


  METHOD run_delta.
    mv_current_mode = c_mode_delta.

    TRY.
        open_extraction( c_mode_delta ).
        rv_records = fetch_and_process( ).
        close_extraction( abap_true ).
        record_success( rv_records ).

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        TRY.
            close_extraction( abap_false ).
          CATCH cx_root.
        ENDTRY.
        record_failure( lx_error->mv_error_text ).
        RAISE EXCEPTION lx_error.
    ENDTRY.
  ENDMETHOD.


  METHOD open_extraction.
    DATA: lt_return     TYPE TABLE OF bapiret2,
          lt_selections TYPE TABLE OF rssdlrange,
          lt_fields     TYPE TABLE OF fieldname.

    mv_current_mode = iv_mode.

    CALL FUNCTION 'RODPS_REPL_ODP_OPEN'
      EXPORTING
        i_subscriber_type         = c_subscriber_type
        i_subscriber_name         = c_subscriber_name
        i_subscriber_process      = mv_process_id
        i_context                 = c_context_sapi
        i_odpname                 = mv_datasource
        i_extraction_mode         = iv_mode
        i_explicit_close          = abap_true
        i_delta_extension_no_data = abap_false
      IMPORTING
        e_pointer                 = mv_pointer
      TABLES
        it_select                 = lt_selections
        it_projection             = lt_fields
        et_return                 = lt_return
      EXCEPTIONS
        OTHERS                    = 1.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_text = |Failed to open ODP extraction for { mv_datasource } (mode={ iv_mode })|.
    ENDIF.

    LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_code = CONV i( ls_return-number )
          iv_error_text = CONV string( ls_return-message ).
    ENDLOOP.
  ENDMETHOD.


  METHOD fetch_and_process.
    DATA: lt_data       TYPE STANDARD TABLE OF char8000,
          lt_return     TYPE TABLE OF bapiret2,
          lv_no_more    TYPE abap_bool.

    rv_records = 0.

    DO.
      CLEAR: lt_data, lt_return, lv_no_more.

      CALL FUNCTION 'RODPS_REPL_ODP_FETCH'
        EXPORTING
          i_pointer        = mv_pointer
          i_maxpackagesize = 52428800
        IMPORTING
          e_no_more_data   = lv_no_more
        TABLES
          et_data          = lt_data
          et_return        = lt_return
        EXCEPTIONS
          OTHERS           = 1.

      IF sy-subrc <> 0.
        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = mv_datasource
            iv_error_text = 'Failed to fetch ODP data package'
            iv_request_id = mv_pointer.
      ENDIF.

      LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = mv_datasource
            iv_error_code = CONV i( ls_return-number )
            iv_error_text = CONV string( ls_return-message )
            iv_request_id = mv_pointer.
      ENDLOOP.

      rv_records = rv_records + lines( lt_data ).

      IF lv_no_more = abap_true.
        EXIT.
      ENDIF.
    ENDDO.
  ENDMETHOD.


  METHOD close_extraction.
    DATA: lt_return TYPE TABLE OF bapiret2.

    IF mv_pointer IS INITIAL.
      RETURN.
    ENDIF.

    CALL FUNCTION 'RODPS_REPL_ODP_CLOSE'
      EXPORTING
        i_pointer   = mv_pointer
        i_confirmed = iv_confirmed
      TABLES
        et_return   = lt_return
      EXCEPTIONS
        OTHERS      = 1.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_text = |Failed to close ODP extraction (confirmed={ iv_confirmed })|
          iv_request_id = mv_pointer.
    ENDIF.

    LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_code = CONV i( ls_return-number )
          iv_error_text = CONV string( ls_return-message )
          iv_request_id = mv_pointer.
    ENDLOOP.
  ENDMETHOD.


  METHOD reset_subscription.
    CALL FUNCTION 'RODPS_REPL_ODP_RESET'
      EXPORTING
        i_subscriber_type = c_subscriber_type
        i_subscriber_name = c_subscriber_name
        i_context         = c_context_sapi
        i_odpname         = mv_datasource
      EXCEPTIONS
        OTHERS            = 1.

    rv_success = COND #( WHEN sy-subrc = 0 THEN abap_true ELSE abap_false ).

    IF rv_success = abap_true.
      UPDATE zbqtr_subsc
        SET status = 'I'
            last_delta_pointer = ''
        WHERE datasource = @mv_datasource.
      COMMIT WORK.
    ENDIF.
  ENDMETHOD.


  METHOD subscription_exists.
    DATA: lv_count TYPE i.

    SELECT COUNT(*) INTO @lv_count
      FROM odqsn
      WHERE subscribertype = @c_subscriber_type
        AND subscribername = @c_subscriber_name
        AND odpname        = @mv_datasource.

    rv_exists = COND #( WHEN lv_count > 0 THEN abap_true ELSE abap_false ).
  ENDMETHOD.


  METHOD get_subscription_status.
    SELECT SINGLE * FROM zbqtr_subsc
      WHERE datasource = @mv_datasource
      INTO @rs_status.
  ENDMETHOD.


  METHOD get_datasource.
    rv_datasource = mv_datasource.
  ENDMETHOD.


  METHOD get_pointer.
    rv_pointer = mv_pointer.
  ENDMETHOD.


  METHOD update_subscription_tracking.
    DATA: ls_sub TYPE zbqtr_subsc.

    SELECT SINGLE * FROM zbqtr_subsc
      WHERE datasource = @mv_datasource
      INTO @ls_sub.

    IF sy-subrc <> 0.
      ls_sub-datasource    = mv_datasource.
      ls_sub-subscriber_id = c_subscriber_name.
      ls_sub-init_date     = sy-datum.
      ls_sub-init_time     = sy-uzeit.
    ENDIF.

    ls_sub-last_delta_date    = sy-datum.
    ls_sub-last_delta_time    = sy-uzeit.
    ls_sub-last_delta_pointer = mv_pointer.
    ls_sub-last_records       = iv_records.
    ls_sub-status             = iv_status.

    IF iv_error IS NOT INITIAL.
      ls_sub-last_error = iv_error.
    ENDIF.

    MODIFY zbqtr_subsc FROM ls_sub.
    COMMIT WORK AND WAIT.
  ENDMETHOD.


  METHOD record_success.
    DATA: ls_sub TYPE zbqtr_subsc.

    SELECT SINGLE * FROM zbqtr_subsc
      WHERE datasource = @mv_datasource
      INTO @ls_sub.

    ls_sub-datasource             = mv_datasource.
    ls_sub-subscriber_id          = c_subscriber_name.
    ls_sub-last_delta_date        = sy-datum.
    ls_sub-last_delta_time        = sy-uzeit.
    ls_sub-last_delta_pointer     = mv_pointer.
    ls_sub-last_records           = iv_records.
    ls_sub-total_records          = ls_sub-total_records + iv_records.
    ls_sub-status                 = 'A'.
    ls_sub-consecutive_failures   = 0.
    CLEAR ls_sub-last_error.

    IF ls_sub-init_date IS INITIAL.
      ls_sub-init_date = sy-datum.
      ls_sub-init_time = sy-uzeit.
    ENDIF.

    MODIFY zbqtr_subsc FROM ls_sub.
    COMMIT WORK AND WAIT.
  ENDMETHOD.


  METHOD record_failure.
    DATA: ls_sub TYPE zbqtr_subsc.

    SELECT SINGLE * FROM zbqtr_subsc
      WHERE datasource = @mv_datasource
      INTO @ls_sub.

    ls_sub-datasource             = mv_datasource.
    ls_sub-subscriber_id          = c_subscriber_name.
    ls_sub-last_delta_date        = sy-datum.
    ls_sub-last_delta_time        = sy-uzeit.
    ls_sub-status                 = 'E'.
    ls_sub-consecutive_failures   = ls_sub-consecutive_failures + 1.
    ls_sub-last_error             = iv_error.

    IF ls_sub-init_date IS INITIAL.
      ls_sub-init_date = sy-datum.
      ls_sub-init_time = sy-uzeit.
    ENDIF.

    MODIFY zbqtr_subsc FROM ls_sub.
    COMMIT WORK AND WAIT.
  ENDMETHOD.

ENDCLASS.
