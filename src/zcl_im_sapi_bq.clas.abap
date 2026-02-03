CLASS zcl_im_sapi_bq DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_ex_rsu5_sapi_badi.

  PRIVATE SECTION.
    CLASS-DATA gt_active_ds TYPE SORTED TABLE OF char30 WITH UNIQUE KEY table_line.
    CLASS-DATA gv_initialized TYPE abap_bool.
    CLASS-DATA gv_test_mode TYPE abap_bool.

    CLASS-METHODS load_active_datasources.

    CLASS-METHODS is_active_datasource
      IMPORTING
        iv_datasource    TYPE char30
      RETURNING
        VALUE(rv_active) TYPE abap_bool.

    CLASS-METHODS get_datasource_config
      IMPORTING
        iv_datasource    TYPE char30
      RETURNING
        VALUE(rs_config) TYPE zbqtr_config.

ENDCLASS.


CLASS zcl_im_sapi_bq IMPLEMENTATION.

  METHOD if_ex_rsu5_sapi_badi~data_transform.
    DATA: lo_replicator TYPE REF TO zcl_bq_replicator,
          ls_result     TYPE zcl_bq_replicator=>ty_result,
          lv_datasource TYPE char30,
          lv_updmode    TYPE char1.

    " Convert parameters to local types
    lv_datasource = i_datasource.
    lv_updmode = i_updmode.

    " Initialize active datasource cache on first call
    IF gv_initialized = abap_false.
      load_active_datasources( ).
    ENDIF.

    " Check if this datasource should be replicated
    IF is_active_datasource( lv_datasource ) = abap_false.
      RETURN.
    ENDIF.

    " Skip if no data to process
    IF c_t_data IS INITIAL.
      RETURN.
    ENDIF.

    " Skip actual replication in test mode
    IF gv_test_mode = abap_true.
      RETURN.
    ENDIF.

    " Get datasource configuration
    DATA(ls_config) = get_datasource_config( lv_datasource ).

    " Check if this is full-only datasource running in delta mode
    IF ls_config-full_only = abap_true AND lv_updmode = 'D'.
      RETURN.
    ENDIF.

    " Replicate data to BigQuery
    TRY.
        lo_replicator = NEW zcl_bq_replicator( iv_datasource = lv_datasource ).

        ls_result = lo_replicator->replicate(
          it_data    = c_t_data
          iv_updmode = lv_updmode ).

        IF ls_result-success = abap_true.
          " Add success message to output
          APPEND VALUE #(
            msgty = 'S'
            msgid = 'ZBQTR'
            msgno = '010'
            msgv1 = lv_datasource
            msgv2 = ls_result-records_sent
          ) TO c_t_messages.
        ENDIF.

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        " Log error message
        APPEND VALUE #(
          msgty = 'E'
          msgid = 'ZBQTR'
          msgno = '011'
          msgv1 = lv_datasource
          msgv2 = lx_error->mv_error_text
        ) TO c_t_messages.

        " Note: We cannot raise custom exceptions from BAdI interface
        " Error is logged and delta will be confirmed
        " For fail-safe behavior, use the scheduler program instead

      CATCH cx_root INTO DATA(lx_root).
        " Unexpected error
        APPEND VALUE #(
          msgty = 'E'
          msgid = 'ZBQTR'
          msgno = '012'
          msgv1 = lv_datasource
          msgv2 = lx_root->get_text( )
        ) TO c_t_messages.
    ENDTRY.
  ENDMETHOD.


  METHOD if_ex_rsu5_sapi_badi~hier_transform.
    " Hierarchy extraction - not used for this solution
  ENDMETHOD.


  METHOD load_active_datasources.
    SELECT datasource FROM zbqtr_config
      WHERE active = 'X'
      INTO TABLE @gt_active_ds.

    gv_initialized = abap_true.
  ENDMETHOD.


  METHOD is_active_datasource.
    READ TABLE gt_active_ds WITH KEY table_line = iv_datasource
      TRANSPORTING NO FIELDS.

    rv_active = COND #( WHEN sy-subrc = 0 THEN abap_true ELSE abap_false ).
  ENDMETHOD.


  METHOD get_datasource_config.
    SELECT SINGLE * FROM zbqtr_config
      WHERE datasource = @iv_datasource
      INTO @rs_config.
  ENDMETHOD.

ENDCLASS.
