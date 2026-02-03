*&---------------------------------------------------------------------*
*& Report Z_BQ_EXTRACTOR_MONITOR
*&---------------------------------------------------------------------*
*& Purpose: Monitor BQ Toolkit replication status
*&          Shows extraction history, pending deltas, and errors
*&
*& Features:
*&   - View replication log (ZBQTR_LOG)
*&   - Check subscription status (ZBQTR_SUBSCRIPTION)
*&   - Query ODQMON for pending deltas
*&   - Alert on consecutive failures
*&
*& Author: SAP BQ Toolkit Integration
*&---------------------------------------------------------------------*
REPORT z_bq_extractor_monitor.

*----------------------------------------------------------------------*
* Selection Screen
*----------------------------------------------------------------------*
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.

PARAMETERS: p_ds    TYPE char30 DEFAULT '*'.  " Datasource filter

SELECT-OPTIONS: s_date FOR sy-datum DEFAULT sy-datum.  " Date range

PARAMETERS: p_status TYPE char1.  " Status filter (S/E/W or blank for all)

SELECTION-SCREEN END OF BLOCK b1.

SELECTION-SCREEN BEGIN OF BLOCK b2 WITH FRAME TITLE TEXT-002.

PARAMETERS: p_log   RADIOBUTTON GROUP rb1 DEFAULT 'X',  " Show log entries
            p_sub   RADIOBUTTON GROUP rb1,              " Show subscriptions
            p_pend  RADIOBUTTON GROUP rb1,              " Show pending deltas
            p_fail  RADIOBUTTON GROUP rb1.              " Show failures only

SELECTION-SCREEN END OF BLOCK b2.

SELECTION-SCREEN BEGIN OF BLOCK b3 WITH FRAME TITLE TEXT-003.

PARAMETERS: p_max TYPE i DEFAULT 100.  " Max rows to display

SELECTION-SCREEN END OF BLOCK b3.


*----------------------------------------------------------------------*
* Text Symbols
*----------------------------------------------------------------------*
* TEXT-001: Selection Criteria
* TEXT-002: Report Type
* TEXT-003: Display Options
* TEXT-010: BQ Toolkit Replication Log
* TEXT-011: Subscription Status
* TEXT-012: Pending Deltas (ODQMON)
* TEXT-013: Consecutive Failures


*----------------------------------------------------------------------*
* Types
*----------------------------------------------------------------------*
TYPES: BEGIN OF ty_log_display,
         log_id              TYPE zbqtr_log-log_id,
         timestamp           TYPE zbqtr_log-timestamp,
         datasource          TYPE zbqtr_log-datasource,
         updmode             TYPE zbqtr_log-updmode,
         records_extracted   TYPE zbqtr_log-records_extracted,
         records_replicated  TYPE zbqtr_log-records_replicated,
         status              TYPE zbqtr_log-status,
         error_message       TYPE zbqtr_log-error_message,
         duration_ms         TYPE zbqtr_log-duration_ms,
         status_icon         TYPE icon_d,
       END OF ty_log_display.

TYPES: BEGIN OF ty_sub_display,
         datasource            TYPE zbqtr_subscription-datasource,
         subscriber_id         TYPE zbqtr_subscription-subscriber_id,
         last_delta_date       TYPE zbqtr_subscription-last_delta_date,
         last_delta_time       TYPE zbqtr_subscription-last_delta_time,
         last_records          TYPE zbqtr_subscription-last_records,
         total_records         TYPE zbqtr_subscription-total_records,
         status                TYPE zbqtr_subscription-status,
         consecutive_failures  TYPE zbqtr_subscription-consecutive_failures,
         last_error            TYPE zbqtr_subscription-last_error,
         status_icon           TYPE icon_d,
       END OF ty_sub_display.


*----------------------------------------------------------------------*
* Data Declarations
*----------------------------------------------------------------------*
DATA: gt_log TYPE TABLE OF ty_log_display,
      gt_sub TYPE TABLE OF ty_sub_display.


*----------------------------------------------------------------------*
* Local Classes
*----------------------------------------------------------------------*
CLASS lcl_monitor DEFINITION.
  PUBLIC SECTION.
    METHODS show_log
      IMPORTING
        iv_datasource_filter TYPE char30
        it_date_range        TYPE RANGE OF sy-datum
        iv_status_filter     TYPE char1
        iv_max_rows          TYPE i.

    METHODS show_subscriptions
      IMPORTING
        iv_datasource_filter TYPE char30.

    METHODS show_pending_deltas
      IMPORTING
        iv_datasource_filter TYPE char30.

    METHODS show_failures
      IMPORTING
        iv_datasource_filter TYPE char30.

  PRIVATE SECTION.
    METHODS get_status_icon
      IMPORTING
        iv_status      TYPE char1
      RETURNING
        VALUE(rv_icon) TYPE icon_d.

    METHODS display_alv
      IMPORTING
        it_data      TYPE STANDARD TABLE
        iv_title     TYPE string
        iv_structure TYPE string.

ENDCLASS.


CLASS lcl_monitor IMPLEMENTATION.

  METHOD show_log.
    DATA: lt_log    TYPE TABLE OF zbqtr_log,
          lv_filter TYPE char30.

    lv_filter = iv_datasource_filter.
    REPLACE ALL OCCURRENCES OF '*' IN lv_filter WITH '%'.

    " Build WHERE clause
    SELECT * FROM zbqtr_log
      WHERE datasource LIKE @lv_filter
        AND timestamp >= @( CONV timestampl( |{ it_date_range[ 1 ]-low }000000| ) )
        AND timestamp <= @( CONV timestampl( |{ it_date_range[ 1 ]-high }235959| ) )
        AND ( @iv_status_filter = '' OR status = @iv_status_filter )
      ORDER BY timestamp DESCENDING
      INTO TABLE @lt_log
      UP TO @iv_max_rows ROWS.

    " Convert to display format
    LOOP AT lt_log INTO DATA(ls_log).
      APPEND VALUE ty_log_display(
        log_id             = ls_log-log_id
        timestamp          = ls_log-timestamp
        datasource         = ls_log-datasource
        updmode            = ls_log-updmode
        records_extracted  = ls_log-records_extracted
        records_replicated = ls_log-records_replicated
        status             = ls_log-status
        error_message      = ls_log-error_message
        duration_ms        = ls_log-duration_ms
        status_icon        = get_status_icon( ls_log-status )
      ) TO gt_log.
    ENDLOOP.

    display_alv(
      it_data      = gt_log
      iv_title     = CONV string( TEXT-010 )
      iv_structure = 'TY_LOG_DISPLAY' ).
  ENDMETHOD.


  METHOD show_subscriptions.
    DATA: lt_sub    TYPE TABLE OF zbqtr_subscription,
          lv_filter TYPE char30.

    lv_filter = iv_datasource_filter.
    REPLACE ALL OCCURRENCES OF '*' IN lv_filter WITH '%'.

    SELECT * FROM zbqtr_subscription
      WHERE datasource LIKE @lv_filter
      ORDER BY datasource
      INTO TABLE @lt_sub.

    " Convert to display format
    LOOP AT lt_sub INTO DATA(ls_sub).
      APPEND VALUE ty_sub_display(
        datasource           = ls_sub-datasource
        subscriber_id        = ls_sub-subscriber_id
        last_delta_date      = ls_sub-last_delta_date
        last_delta_time      = ls_sub-last_delta_time
        last_records         = ls_sub-last_records
        total_records        = ls_sub-total_records
        status               = ls_sub-status
        consecutive_failures = ls_sub-consecutive_failures
        last_error           = ls_sub-last_error
        status_icon          = get_status_icon( ls_sub-status )
      ) TO gt_sub.
    ENDLOOP.

    display_alv(
      it_data      = gt_sub
      iv_title     = CONV string( TEXT-011 )
      iv_structure = 'TY_SUB_DISPLAY' ).
  ENDMETHOD.


  METHOD show_pending_deltas.
    " Query ODQMON for pending (unconfirmed) deltas
    DATA: lv_filter TYPE char30.

    lv_filter = iv_datasource_filter.
    REPLACE ALL OCCURRENCES OF '*' IN lv_filter WITH '%'.

    WRITE: / TEXT-012.
    ULINE.

    " Query ODP subscription table for our subscriber
    SELECT odpname, COUNT(*) AS count
      FROM odqrequesth
      WHERE subscribertype = @zcl_bq_odp_subscriber=>c_subscriber_type
        AND subscribername = @zcl_bq_odp_subscriber=>c_subscriber_name
        AND odpname LIKE @lv_filter
        AND status <> 'C'  " Not confirmed
      GROUP BY odpname
      INTO TABLE @DATA(lt_pending).

    IF lt_pending IS INITIAL.
      WRITE: / 'No pending deltas found.'.
    ELSE.
      LOOP AT lt_pending INTO DATA(ls_pending).
        WRITE: / |Datasource: { ls_pending-odpname } - Pending requests: { ls_pending-count }|.
      ENDLOOP.
    ENDIF.

    ULINE.
    WRITE: / 'Use transaction ODQMON for detailed delta queue analysis.'.
  ENDMETHOD.


  METHOD show_failures.
    " Show datasources with consecutive failures
    DATA: lv_filter TYPE char30.

    lv_filter = iv_datasource_filter.
    REPLACE ALL OCCURRENCES OF '*' IN lv_filter WITH '%'.

    WRITE: / TEXT-013.
    ULINE.

    SELECT * FROM zbqtr_subscription
      WHERE datasource LIKE @lv_filter
        AND consecutive_failures > 0
      ORDER BY consecutive_failures DESCENDING
      INTO TABLE @DATA(lt_failures).

    IF lt_failures IS INITIAL.
      WRITE: / 'No consecutive failures detected.'.
    ELSE.
      LOOP AT lt_failures INTO DATA(ls_fail).
        WRITE: / |{ ls_fail-datasource }: { ls_fail-consecutive_failures } consecutive failures|.
        WRITE: / |  Last error: { ls_fail-last_error }|.
        WRITE: / |  Last attempt: { ls_fail-last_delta_date } { ls_fail-last_delta_time }|.
        ULINE.
      ENDLOOP.

      " Alert threshold
      DATA(lv_critical) = REDUCE i( INIT c = 0 FOR f IN lt_failures WHERE ( consecutive_failures >= 3 ) NEXT c = c + 1 ).
      IF lv_critical > 0.
        WRITE: / |WARNING: { lv_critical } datasource(s) have 3+ consecutive failures!|.
      ENDIF.
    ENDIF.
  ENDMETHOD.


  METHOD get_status_icon.
    rv_icon = SWITCH #( iv_status
      WHEN 'S' OR 'A' THEN icon_green_light
      WHEN 'W'        THEN icon_yellow_light
      WHEN 'E'        THEN icon_red_light
      WHEN 'I'        THEN icon_led_inactive
      ELSE                 icon_led_inactive ).
  ENDMETHOD.


  METHOD display_alv.
    DATA: lo_alv    TYPE REF TO cl_salv_table,
          lx_error  TYPE REF TO cx_salv_msg.

    TRY.
        cl_salv_table=>factory(
          IMPORTING
            r_salv_table = lo_alv
          CHANGING
            t_table      = it_data ).

        " Set title
        lo_alv->get_display_settings( )->set_list_header( CONV #( iv_title ) ).

        " Enable all standard functions
        lo_alv->get_functions( )->set_all( abap_true ).

        " Optimize column widths
        lo_alv->get_columns( )->set_optimize( abap_true ).

        " Display
        lo_alv->display( ).

      CATCH cx_salv_msg INTO lx_error.
        MESSAGE lx_error TYPE 'E'.
    ENDTRY.
  ENDMETHOD.

ENDCLASS.


*----------------------------------------------------------------------*
* Main Program
*----------------------------------------------------------------------*
START-OF-SELECTION.

  DATA(lo_monitor) = NEW lcl_monitor( ).

  CASE abap_true.
    WHEN p_log.
      lo_monitor->show_log(
        iv_datasource_filter = p_ds
        it_date_range        = s_date[]
        iv_status_filter     = p_status
        iv_max_rows          = p_max ).

    WHEN p_sub.
      lo_monitor->show_subscriptions( iv_datasource_filter = p_ds ).

    WHEN p_pend.
      lo_monitor->show_pending_deltas( iv_datasource_filter = p_ds ).

    WHEN p_fail.
      lo_monitor->show_failures( iv_datasource_filter = p_ds ).
  ENDCASE.
