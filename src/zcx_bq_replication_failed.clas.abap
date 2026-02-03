"! <p class="shorttext synchronized">Exception: BigQuery Replication Failed</p>
"! Custom exception class for BQ replication failures.
"! When raised, this exception prevents ODP delta confirmation,
"! ensuring failed data remains in the queue for retry.
CLASS zcx_bq_replication_failed DEFINITION
  PUBLIC
  INHERITING FROM cx_static_check
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_t100_message.

    CONSTANTS:
      BEGIN OF gc_msgid,
        msgid TYPE symsgid VALUE 'ZBQTR',
      END OF gc_msgid.

    CONSTANTS:
      BEGIN OF bq_api_error,
        msgid TYPE symsgid VALUE 'ZBQTR',
        msgno TYPE symsgno VALUE '001',
        attr1 TYPE scx_attrname VALUE 'MV_DATASOURCE',
        attr2 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF bq_api_error,
      BEGIN OF odp_open_error,
        msgid TYPE symsgid VALUE 'ZBQTR',
        msgno TYPE symsgno VALUE '002',
        attr1 TYPE scx_attrname VALUE 'MV_DATASOURCE',
        attr2 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF odp_open_error,
      BEGIN OF odp_fetch_error,
        msgid TYPE symsgid VALUE 'ZBQTR',
        msgno TYPE symsgno VALUE '003',
        attr1 TYPE scx_attrname VALUE 'MV_DATASOURCE',
        attr2 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF odp_fetch_error,
      BEGIN OF odp_close_error,
        msgid TYPE symsgid VALUE 'ZBQTR',
        msgno TYPE symsgno VALUE '004',
        attr1 TYPE scx_attrname VALUE 'MV_DATASOURCE',
        attr2 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF odp_close_error,
      BEGIN OF config_not_found,
        msgid TYPE symsgid VALUE 'ZBQTR',
        msgno TYPE symsgno VALUE '005',
        attr1 TYPE scx_attrname VALUE 'MV_DATASOURCE',
        attr2 TYPE scx_attrname VALUE '',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF config_not_found,
      BEGIN OF bq_connection_error,
        msgid TYPE symsgid VALUE 'ZBQTR',
        msgno TYPE symsgno VALUE '006',
        attr1 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr2 TYPE scx_attrname VALUE '',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF bq_connection_error.

    "! DataSource name where error occurred
    DATA mv_datasource TYPE char30 READ-ONLY.

    "! Error code from BQ Toolkit or ODP
    DATA mv_error_code TYPE i READ-ONLY.

    "! Detailed error message
    DATA mv_error_text TYPE string READ-ONLY.

    "! Request ID for tracking
    DATA mv_request_id TYPE rodps_repl_pointer READ-ONLY.

    "! <p class="shorttext synchronized">Constructor</p>
    "!
    "! @parameter iv_datasource | DataSource name
    "! @parameter iv_error_code | Error code
    "! @parameter iv_error_text | Error message text
    "! @parameter iv_request_id | ODP request ID
    "! @parameter textid | Message text ID
    "! @parameter previous | Previous exception
    METHODS constructor
      IMPORTING
        iv_datasource TYPE char30 OPTIONAL
        iv_error_code TYPE i OPTIONAL
        iv_error_text TYPE string OPTIONAL
        iv_request_id TYPE rodps_repl_pointer OPTIONAL
        textid        LIKE if_t100_message=>t100key OPTIONAL
        previous      TYPE REF TO cx_root OPTIONAL.

    "! <p class="shorttext synchronized">Get formatted error message</p>
    "!
    "! @parameter rv_message | Formatted error message
    METHODS get_error_message
      RETURNING VALUE(rv_message) TYPE string.

ENDCLASS.


CLASS zcx_bq_replication_failed IMPLEMENTATION.

  METHOD constructor ##ADT_SUPPRESS_GENERATION.
    super->constructor( previous = previous ).

    mv_datasource = iv_datasource.
    mv_error_code = iv_error_code.
    mv_error_text = iv_error_text.
    mv_request_id = iv_request_id.

    CLEAR me->textid.
    IF textid IS INITIAL.
      if_t100_message~t100key = bq_api_error.
    ELSE.
      if_t100_message~t100key = textid.
    ENDIF.
  ENDMETHOD.


  METHOD get_error_message.
    rv_message = |BQ Replication Error for { mv_datasource }: { mv_error_text } (Code: { mv_error_code })|.
    IF mv_request_id IS NOT INITIAL.
      rv_message = rv_message && | [Request: { mv_request_id }]|.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
