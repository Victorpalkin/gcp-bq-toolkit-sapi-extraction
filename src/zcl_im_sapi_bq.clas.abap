CLASS zcl_im_sapi_bq DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_ex_rsu5_sapi_badi.

  PRIVATE SECTION.
    CLASS-DATA gv_test_mode TYPE abap_bool.

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

    " Check if datasource was initialized by us (exists in ZBQTR_CONFIG)
    DATA(ls_config) = get_datasource_config( lv_datasource ).
    IF ls_config IS INITIAL.
      " Not our datasource - skip replication
      RETURN.
    ENDIF.

    " Verify this is our subscription (check subscriber type/name match)
    IF ls_config-subscriber_type <> zcl_bq_odp_subscriber=>c_subscriber_type OR
       ls_config-subscriber_name <> zcl_bq_odp_subscriber=>c_subscriber_name.
      " Not our subscription - skip replication
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

    " Check if this is full-only datasource running in delta mode
    IF ls_config-full_only = abap_true AND lv_updmode = 'D'.
      RETURN.
    ENDIF.

    " Replicate data to BigQuery using stored config
    TRY.
        lo_replicator = NEW zcl_bq_replicator(
          iv_datasource  = lv_datasource
          iv_mass_tr_key = ls_config-mass_tr_key
          iv_struct_name = ls_config-struct_name ).

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


  METHOD get_datasource_config.
    SELECT SINGLE * FROM zbqtr_config
      WHERE datasource = @iv_datasource
      INTO @rs_config.
  ENDMETHOD.

ENDCLASS.
