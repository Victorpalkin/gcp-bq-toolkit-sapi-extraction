"! <p class="shorttext synchronized">BAdI Implementation: RSU5_SAPI_BADI for BQ Replication</p>
"! Intercepts S-API extractor data and replicates to BigQuery.
"! If BQ replication fails, raises exception to prevent delta confirmation.
CLASS zcl_im_sapi_bq DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    "! BAdI interface for S-API extraction enhancement
    INTERFACES if_ex_rsu5_sapi_badi.

  PRIVATE SECTION.
    "! Cached list of active datasources
    CLASS-DATA gt_active_ds TYPE SORTED TABLE OF char30 WITH UNIQUE KEY table_line.

    "! Flag indicating if cache is loaded
    CLASS-DATA gv_initialized TYPE abap_bool.

    "! Test mode flag (skips BQ call)
    CLASS-DATA gv_test_mode TYPE abap_bool.

    "! Load active datasources from configuration
    CLASS-METHODS load_active_datasources.

    "! Check if datasource should be processed
    CLASS-METHODS is_active_datasource
      IMPORTING
        iv_datasource     TYPE roosourcer
      RETURNING
        VALUE(rv_active) TYPE abap_bool.

    "! Get configuration for datasource
    CLASS-METHODS get_datasource_config
      IMPORTING
        iv_datasource    TYPE char30
      RETURNING
        VALUE(rs_config) TYPE zbqtr_config.

ENDCLASS.


CLASS zcl_im_sapi_bq IMPLEMENTATION.

  METHOD if_ex_rsu5_sapi_badi~data_transform.
    " -----------------------------------------------------------------------
    " DATA_TRANSFORM Method - Main entry point for S-API data interception
    " -----------------------------------------------------------------------
    " Parameters:
    "   I_DATASOURCE - DataSource name (e.g., 2LIS_02_SGR)
    "   I_UPDMODE    - Update mode: F=Full, D=Delta, R=Repeat, I=Init
    "   I_T_SELECT   - Selection parameters
    "   I_T_FIELDS   - Field list
    "   C_T_DATA     - Data table (CHANGING)
    "   C_T_MESSAGES - Messages
    " -----------------------------------------------------------------------

    DATA: lo_replicator TYPE REF TO zcl_bq_replicator,
          ls_result     TYPE zcl_bq_replicator=>ty_result.

    " Initialize active datasource cache on first call
    IF gv_initialized = abap_false.
      load_active_datasources( ).
    ENDIF.

    " Check if this datasource should be replicated
    IF is_active_datasource( i_datasource ) = abap_false.
      RETURN.  " Not our datasource, skip processing
    ENDIF.

    " Skip if no data to process
    IF c_t_data IS INITIAL.
      RETURN.
    ENDIF.

    " Skip actual replication in test mode
    IF gv_test_mode = abap_true.
      MESSAGE i000(zbqtr) WITH 'Test mode: Skipping BQ replication for' i_datasource.
      RETURN.
    ENDIF.

    " Get datasource configuration
    DATA(ls_config) = get_datasource_config( CONV char30( i_datasource ) ).

    " Check if this is full-only datasource running in delta mode
    IF ls_config-full_only = abap_true AND i_updmode = 'D'.
      " Skip delta for full-only datasources
      RETURN.
    ENDIF.

    " Replicate data to BigQuery
    TRY.
        lo_replicator = NEW zcl_bq_replicator( iv_datasource = CONV char30( i_datasource ) ).

        ls_result = lo_replicator->replicate(
          it_data    = c_t_data
          iv_updmode = i_updmode ).

        IF ls_result-success = abap_false.
          " Raise exception to prevent delta confirmation
          RAISE EXCEPTION TYPE zcx_bq_replication_failed
            EXPORTING
              iv_datasource = CONV char30( i_datasource )
              iv_error_code = ls_result-error_code
              iv_error_text = ls_result-error_message.
        ENDIF.

        " Add success message to output
        APPEND VALUE #(
          msgty = 'S'
          msgid = 'ZBQTR'
          msgno = '010'
          msgv1 = i_datasource
          msgv2 = ls_result-records_sent
        ) TO c_t_messages.

      CATCH zcx_bq_replication_failed INTO DATA(lx_error).
        " Log error message
        APPEND VALUE #(
          msgty = 'E'
          msgid = 'ZBQTR'
          msgno = '011'
          msgv1 = i_datasource
          msgv2 = lx_error->mv_error_text
        ) TO c_t_messages.

        " Write to system log for visibility
        MESSAGE e011(zbqtr) WITH i_datasource lx_error->mv_error_text.

        " Re-raise to prevent delta confirmation
        " The ODP framework will NOT confirm the delta
        RAISE EXCEPTION lx_error.

      CATCH cx_root INTO DATA(lx_root).
        " Unexpected error - also prevent confirmation
        APPEND VALUE #(
          msgty = 'E'
          msgid = 'ZBQTR'
          msgno = '012'
          msgv1 = i_datasource
          msgv2 = lx_root->get_text( )
        ) TO c_t_messages.

        MESSAGE e012(zbqtr) WITH i_datasource lx_root->get_text( ).

        RAISE EXCEPTION TYPE zcx_bq_replication_failed
          EXPORTING
            iv_datasource = CONV char30( i_datasource )
            iv_error_text = lx_root->get_text( )
            previous      = lx_root.
    ENDTRY.
  ENDMETHOD.


  METHOD if_ex_rsu5_sapi_badi~hier_transform.
    " -----------------------------------------------------------------------
    " HIER_TRANSFORM Method - Hierarchy extraction (not used for this solution)
    " -----------------------------------------------------------------------
    " This method is called for hierarchy extractions.
    " We don't process hierarchy data, so this is a no-op.
  ENDMETHOD.


  METHOD load_active_datasources.
    " Load all active datasources from configuration table
    SELECT datasource FROM zbqtr_config
      WHERE active = 'X'
      INTO TABLE @gt_active_ds.

    gv_initialized = abap_true.
  ENDMETHOD.


  METHOD is_active_datasource.
    " Check if datasource is in our active list
    READ TABLE gt_active_ds WITH KEY table_line = CONV char30( iv_datasource )
      TRANSPORTING NO FIELDS.

    rv_active = COND #( WHEN sy-subrc = 0 THEN abap_true ELSE abap_false ).
  ENDMETHOD.


  METHOD get_datasource_config.
    SELECT SINGLE * FROM zbqtr_config
      WHERE datasource = @iv_datasource
      INTO @rs_config.
  ENDMETHOD.

ENDCLASS.
