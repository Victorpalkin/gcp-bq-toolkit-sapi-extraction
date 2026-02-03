"! <p class="shorttext synchronized">ODP Subscription Manager for BQ Replication</p>
"! Manages ODP subscriptions for S-API extractors, handling
"! full loads, deltas, and fail-safe delta confirmation.
CLASS zcl_bq_odp_subscriber DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    "! Subscriber type for ODQMON identification
    CONSTANTS c_subscriber_type TYPE char10 VALUE 'ZBQTR'.

    "! Subscriber name for ODQMON identification
    CONSTANTS c_subscriber_name TYPE char30 VALUE 'ZBQTR_SUBSCRIBER'.

    "! ODP Context for S-API extractors
    CONSTANTS c_context_sapi TYPE char10 VALUE 'SAPI'.

    "! Extraction modes
    CONSTANTS:
      c_mode_full     TYPE char1 VALUE 'F',  " Full extraction
      c_mode_delta    TYPE char1 VALUE 'D',  " Delta extraction
      c_mode_init     TYPE char1 VALUE 'I',  " Initialize without data
      c_mode_recovery TYPE char1 VALUE 'R'.  " Recovery/repeat delta

    "! <p class="shorttext synchronized">Constructor</p>
    "!
    "! @parameter iv_datasource | DataSource name (e.g., 2LIS_02_SGR)
    METHODS constructor
      IMPORTING
        iv_datasource TYPE char30.

    "! <p class="shorttext synchronized">Initialize ODP subscription</p>
    "! Creates subscription in ODQMON (first time setup)
    "!
    "! @parameter rv_success | True if initialization successful
    "! @raising zcx_bq_replication_failed | If initialization fails
    METHODS initialize_subscription
      RETURNING VALUE(rv_success) TYPE abap_bool
      RAISING zcx_bq_replication_failed.

    "! <p class="shorttext synchronized">Run full extraction</p>
    "! Extracts all data from the S-API extractor
    "!
    "! @parameter rv_records | Number of records extracted
    "! @raising zcx_bq_replication_failed | If extraction fails
    METHODS run_full
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    "! <p class="shorttext synchronized">Run delta extraction</p>
    "! Extracts only changed data since last confirmed delta
    "!
    "! @parameter rv_records | Number of records extracted
    "! @raising zcx_bq_replication_failed | If extraction fails
    METHODS run_delta
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    "! <p class="shorttext synchronized">Reset ODP subscription</p>
    "! Clears delta pointer, next delta will be like init
    "!
    "! @parameter rv_success | True if reset successful
    METHODS reset_subscription
      RETURNING VALUE(rv_success) TYPE abap_bool.

    "! <p class="shorttext synchronized">Check if subscription exists in ODQMON</p>
    "!
    "! @parameter rv_exists | True if subscription exists
    METHODS subscription_exists
      RETURNING VALUE(rv_exists) TYPE abap_bool.

    "! <p class="shorttext synchronized">Get subscription status from tracking table</p>
    "!
    "! @parameter rs_status | Subscription status record
    METHODS get_subscription_status
      RETURNING VALUE(rs_status) TYPE zbqtr_subsc.

    "! <p class="shorttext synchronized">Get current datasource</p>
    "!
    "! @parameter rv_datasource | DataSource name
    METHODS get_datasource
      RETURNING VALUE(rv_datasource) TYPE char30.

    "! <p class="shorttext synchronized">Get last extraction pointer</p>
    "!
    "! @parameter rv_pointer | ODP pointer value
    METHODS get_pointer
      RETURNING VALUE(rv_pointer) TYPE char32.

  PRIVATE SECTION.
    DATA mv_datasource TYPE char30.
    DATA mv_pointer TYPE char32.
    DATA mv_process_id TYPE char30.
    DATA mv_current_mode TYPE char1.

    "! Open ODP extraction
    METHODS open_extraction
      IMPORTING
        iv_mode TYPE char1
      RAISING zcx_bq_replication_failed.

    "! Fetch and process all data packages
    METHODS fetch_and_process
      RETURNING VALUE(rv_records) TYPE i
      RAISING zcx_bq_replication_failed.

    "! Close ODP extraction with confirmation flag
    METHODS close_extraction
      IMPORTING
        iv_confirmed TYPE abap_bool
      RAISING zcx_bq_replication_failed.

    "! Update subscription tracking table
    METHODS update_subscription_tracking
      IMPORTING
        iv_status  TYPE char1
        iv_records TYPE i
        iv_error   TYPE string OPTIONAL.

    "! Generate unique process ID for this run
    METHODS generate_process_id
      RETURNING VALUE(rv_id) TYPE char30.

    "! Record failure and check consecutive failure count
    METHODS record_failure
      IMPORTING
        iv_error TYPE string.

    "! Reset failure counter on success
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
    " Generate unique process ID: ZBQTR_YYYYMMDD_HHMMSS_RND
    DATA: lv_random TYPE i.

    CALL FUNCTION 'GENERAL_GET_RANDOM_INT'
      EXPORTING
        range  = 9999
      IMPORTING
        random = lv_random.

    rv_id = |ZBQTR_{ sy-datum }_{ sy-uzeit }_{ lv_random }|.
  ENDMETHOD.


  METHOD initialize_subscription.
    " Initialize delta subscription (first time setup)
    " This creates the subscription in ODQMON without extracting data
    TRY.
        open_extraction( c_mode_init ).
        close_extraction( abap_true ).

        " Create tracking record
        DATA: ls_sub TYPE zbqtr_subsc.
        ls_sub-datasource     = mv_datasource.
        ls_sub-subscriber_id  = c_subscriber_name.
        ls_sub-init_date      = sy-datum.
        ls_sub-init_time      = sy-uzeit.
        ls_sub-status         = 'A'.  " Active

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
        " 1. Open full extraction
        open_extraction( c_mode_full ).

        " 2. Fetch all data (BAdI intercepts and sends to BQ)
        rv_records = fetch_and_process( ).

        " 3. Confirm extraction (only if we get here without exception)
        close_extraction( abap_true ).

        " 4. Update tracking
        record_success( rv_records ).

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        " BQ failed - do NOT confirm
        TRY.
            close_extraction( abap_false ).  " Cancel/rollback
          CATCH cx_root.
            " Ignore close errors
        ENDTRY.
        record_failure( lx_error->mv_error_text ).
        RAISE EXCEPTION lx_error.  " Re-raise original error
    ENDTRY.
  ENDMETHOD.


  METHOD run_delta.
    mv_current_mode = c_mode_delta.

    TRY.
        " 1. Open delta extraction
        open_extraction( c_mode_delta ).

        " 2. Fetch all data (BAdI intercepts and sends to BQ)
        rv_records = fetch_and_process( ).

        " 3. Confirm delta (only if we get here without exception)
        close_extraction( abap_true ).

        " 4. Update tracking
        record_success( rv_records ).

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        " BQ failed - do NOT confirm delta
        TRY.
            close_extraction( abap_false ).  " Cancel/rollback
          CATCH cx_root.
            " Ignore close errors
        ENDTRY.
        record_failure( lx_error->mv_error_text ).
        RAISE EXCEPTION lx_error.  " Re-raise original error
    ENDTRY.
  ENDMETHOD.


  METHOD open_extraction.
    DATA: lt_return     TYPE TABLE OF bapiret2,
          lt_selections TYPE TABLE OF rssdlrange,
          lt_fields     TYPE TABLE OF rsfieldtxt.

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
          iv_error_text = |Failed to open ODP extraction for { mv_datasource } (mode={ iv_mode })|
          textid        = zcx_bq_replication_failed=>odp_open_error.
    ENDIF.

    " Check for errors in return table
    LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_code = CONV i( ls_return-number )
          iv_error_text = ls_return-message
          textid        = zcx_bq_replication_failed=>odp_open_error.
    ENDLOOP.
  ENDMETHOD.


  METHOD fetch_and_process.
    DATA: lt_data       TYPE STANDARD TABLE OF char8000,
          lt_return     TYPE TABLE OF bapiret2,
          lv_no_more    TYPE abap_bool.

    rv_records = 0.

    " Fetch loop - ODP framework calls BAdI DATA_TRANSFORM for each package
    DO.
      CLEAR: lt_data, lt_return, lv_no_more.

      CALL FUNCTION 'RODPS_REPL_ODP_FETCH'
        EXPORTING
          i_pointer        = mv_pointer
          i_maxpackagesize = 52428800  " 50 MB max package size
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
            iv_request_id = mv_pointer
            textid        = zcx_bq_replication_failed=>odp_fetch_error.
      ENDIF.

      " Check for errors in return table
      LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = mv_datasource
            iv_error_code = CONV i( ls_return-number )
            iv_error_text = ls_return-message
            iv_request_id = mv_pointer
            textid        = zcx_bq_replication_failed=>odp_fetch_error.
      ENDLOOP.

      " Count records (BAdI RSU5_SAPI_BADI is called by framework)
      " The BAdI intercepts and sends data to BigQuery
      rv_records = rv_records + lines( lt_data ).

      IF lv_no_more = abap_true.
        EXIT.
      ENDIF.
    ENDDO.
  ENDMETHOD.


  METHOD close_extraction.
    DATA: lt_return TYPE TABLE OF bapiret2.

    IF mv_pointer IS INITIAL.
      RETURN.  " Nothing to close
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
          iv_request_id = mv_pointer
          textid        = zcx_bq_replication_failed=>odp_close_error.
    ENDIF.

    " Check for errors in return table
    LOOP AT lt_return INTO DATA(ls_return) WHERE type CA 'EAX'.
      RAISE EXCEPTION TYPE zcx_bq_replication_failed
        EXPORTING
          iv_datasource = mv_datasource
          iv_error_code = CONV i( ls_return-number )
          iv_error_text = ls_return-message
          iv_request_id = mv_pointer
          textid        = zcx_bq_replication_failed=>odp_close_error.
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
      " Update tracking table
      UPDATE zbqtr_subsc
        SET status = 'I'
            last_delta_pointer = ''
        WHERE datasource = @mv_datasource.
      COMMIT WORK.
    ENDIF.
  ENDMETHOD.


  METHOD subscription_exists.
    " Check ODQMON tables for existing subscription
    DATA: lv_count TYPE i.

    " Try to check ODP subscription table (ODQSN)
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

    " Read existing record
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
    ls_sub-status                 = 'A'.  " Active
    ls_sub-consecutive_failures   = 0.    " Reset on success
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
    ls_sub-status                 = 'E'.  " Error
    ls_sub-consecutive_failures   = ls_sub-consecutive_failures + 1.
    ls_sub-last_error             = iv_error.

    IF ls_sub-init_date IS INITIAL.
      ls_sub-init_date = sy-datum.
      ls_sub-init_time = sy-uzeit.
    ENDIF.

    MODIFY zbqtr_subsc FROM ls_sub.
    COMMIT WORK AND WAIT.

    " Alert if consecutive failures exceed threshold
    IF ls_sub-consecutive_failures >= 3.
      " Log to system log (SM21)
      MESSAGE e001(zbqtr) WITH mv_datasource ls_sub-consecutive_failures INTO DATA(lv_msg).
      " Could also send email notification here
    ENDIF.
  ENDMETHOD.

ENDCLASS.
