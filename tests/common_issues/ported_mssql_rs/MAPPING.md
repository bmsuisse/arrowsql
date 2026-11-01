# microsoft/mssql-rs tests -> arrowsql

Upstream: `github.com/microsoft/mssql-rs` at rev `6b6b48e` (MIT). The Python tests under
`mssql-py-core/tests` target Microsoft's `mssql_py_core` API; the Rust integration tests under
`mssql-tds/tests` target the `mssql-tds` crate. Neither runs here as is. This file maps each
upstream test file to the arrowsql test that covers the same behaviour, or says why not.

Test locations:

- `M` = `tests/mock/` (no SQL Server; runs on every CI job through the mock TDS server)
- `P` = `tests/common_issues/ported_mssql_rs/` (this directory, real SQL Server)
- `E` = tests that existed before this port in `tests/common_issues/ported/`
  (`test_port_mssql_rs_bulkcopy.py`, `test_port_mssql_rs_arrow_bulkcopy.py`) or `tests/common_issues/`

## mssql-py-core/tests (Python)

| Upstream | arrowsql | Notes |
|---|---|---|
| `test_bulkcopy_{tinyint,smallint,int,bigint,bit,float,decimal,numeric,money,char,nchar,nvarchar,binary,date,time,datetime,datetime2,smalldatetime,datetimeoffset,uuid,text,ntext,image,xml,variant}.py` | P `test_bulk_types_and_errors.py::test_bulkcopy_type_round_trip[*]`, `test_bulkcopy_xml_round_trip`, `test_bulkcopy_sql_variant` | one parametrized matrix over `Cursor.bulkcopy`; text/ntext/image are deprecated types, covered via varchar/nvarchar(max)/varbinary(max) |
| `test_bulkcopy_arrow_*.py` (type files) | E `tests/common_issues/test_rs_bulk_native.py`, `test_rs_arrow_type_mapping.py`, ported `test_port_mssql_rs_arrow_bulkcopy.py` | Arrow path already covered |
| `test_bulkcopy_arrow_{longhaul,source_coercion,unsupported_types,type_edges,column_mismatch,options,transactions,multipart_table_names}.py` | E `test_rs_bulk_native*.py`, `test_rs_bulk_parallel*.py`, ported `test_port_mssql_rs_arrow_bulkcopy.py` | `longhaul` skipped: multi-minute soak, not CI material |
| `test_bulkcopy_json.py`, `test_bulkcopy_arrow_json.py`, `test_bulkcopy_vector.py` | P `test_vector_column_is_either_loaded_or_rejected_cleanly`, `test_json_column_is_either_loaded_or_rejected_cleanly` | skip on SQL Server < 2025 (no json/vector type) |
| `test_bulkcopy_varchar_collations.py`, `test_bulkcopy_char_collations.py` | P `test_varchar_collation_encodings[*]` | |
| `test_bulkcopy_multipart_table_names.py` | P `test_table_name_forms[*]`, `test_three_part_table_name_with_current_database`, `test_special_characters_in_table_and_column_names` | |
| `test_bulkcopy_varying_columns.py`, `test_bulkcopy_auto_fewer_columns.py`, `test_bulkcopy_column_mismatch.py` | P `test_wide_and_narrow_tables[*]`; E `test_port_mssql_rs_bulkcopy.py` | |
| `test_bulkcopy_options.py`, `test_bulkcopy_transactions.py`, `test_bulkcopy_rows_copied_accuracy.py`, `test_bulkcopy_timeout_zero.py` | E `test_port_mssql_rs_bulkcopy.py` | |
| `test_bulkcopy_errors_timeouts.py` | P `test_bulk_timeout_udt.py::test_bulkcopy_times_out_when_table_is_locked_and_connection_recovers`, `test_bulk_types_and_errors.py::test_failed_bulkcopy_leaves_connection_usable_and_next_load_succeeds`, `test_string_too_long_for_column_raises_and_connection_recovers`, `test_bulkcopy_into_missing_table_raises` | |
| `test_bulkcopy_datetime_strings.py` | E `tests/common_issues/test_rs_date_strings_dateformat.py` | |
| `test_bulkcopy_longhaul.py` | skipped | soak test |
| `mssql_python/test_bulkcopy*.py` (batch processing, column mappings, unicode) | P `test_column_mappings_by_name_and_by_ordinal`, `test_column_mapping_to_unknown_column_raises`; E `tests/upstream/test_019_bulkcopy.py` and the rest of the upstream suite | arrowsql runs microsoft/mssql-python's own tests |
| `mssql_python/test_bulkcopy_access_token.py`, `test_auth_resolution_e2e.py`, `test_connstr_params_e2e.py`, `test_auth_resolution.py`, `test_entra_id_token_factory.py` | M `test_login_tls.py::test_fedauth_access_token_is_sent_verbatim`, `test_malformed_access_token_is_rejected_before_connecting`; E `test_rs_auth_keywords.py` | real Entra flows need Azure: skipped |
| `rs-only-tests/test_mock_server_fedauth.py` | M `test_login_tls.py` (token flow and the token the server received) | |
| `test_connection.py`, `test_cursor.py`, `test_integration.py` | P `test_session_and_rpc.py` (cursor ops, params, rpc, transactions); E `tests/upstream` | the mssql_py_core object model itself (`PyCoreConnection`, `PyCoreCursor`) has no arrowsql counterpart |
| `test_async_*.py` (connection, cursor, description, execute, executemany, fetch, runtime) | E `tests/common_issues/test_rs_aio*.py` | arrowsql's async API is `arrowsql.aio` |
| `test_client_context.py`, `test_driver_info.py` | skipped | `mssql_py_core` ClientContext / driver-info objects; arrowsql builds its context from the ODBC connection string (user agent checked in M `test_user_password_login`) |
| `smoke/test_smoke.py` | skipped | Docker smoke test of the upstream wheel |

## mssql-tds/tests (Rust)

| Upstream | arrowsql | Notes |
|---|---|---|
| `test_reset_connection.rs` | P `test_pooled_connection_is_reset_between_uses`, `test_open_transaction_is_rolled_back_when_connection_returns_to_pool` | |
| `timeout_and_cancel.rs` | P `test_query_timeout_then_connection_reusable`, `test_cancel_during_waitfor_then_next_statement_works`, `test_timeout_*`; M `test_faults.py` (query timeout, cancel, login delay) | |
| `test_bulk_copy.rs`, `test_bulk_copy_with_metadata.rs` | P bulk type matrix, column mappings | |
| `test_bulk_copy_bigint.rs` | P `test_bigint_extremes_in_key_column` | |
| `test_bulk_copy_empty_plp.rs` | P `test_empty_values_are_stored_as_empty_not_null[*]` | |
| `test_bulk_copy_udt.rs` | P `test_hierarchyid_udt_bulkcopy_as_bytes` | hierarchyid needs no CLR setup |
| `test_bulk_copy_variant.rs`, `test_bulk_copy_uuid.rs`, `test_bulk_copy_xml.rs` | P `test_bulkcopy_sql_variant`, uniqueidentifier case, `test_bulkcopy_xml_round_trip` | xml caught a bug (see below) |
| `test_bulk_copy_vector.rs` | P vector test | SQL Server 2025 only |
| `test_bulk_copy_error_recovery.rs` | P `test_failed_bulkcopy_leaves_connection_usable_and_next_load_succeeds` and siblings | |
| `test_bulk_copy_transactions.rs`, `test_bulk_copy_timeout.rs`, `test_bulk_copy_identifiers.rs`, `test_bulk_copy_varying_columns.rs` | E `test_port_mssql_rs_bulkcopy.py`; P table-name and width tests, lock-timeout test | |
| `test_rpc_datatypes.rs`, `test_narrow_param_encoding.rs` | P `test_integer_parameters_round_trip_at_width_boundaries[*]`, `test_parameter_round_trip[*]`, `test_many_parameters` | |
| `test_rpc_results.rs`, `test_drain_on_error.rs` | P `test_stored_procedure_result_sets_and_return_status`, `test_output_parameter_via_declared_variable`, `test_error_inside_procedure_after_first_result`, `test_error_after_rows_is_raised_and_connection_drains` | |
| `test_tvp.rs` | P `test_list_of_tuples_is_not_silently_accepted_as_tvp` | arrowsql has no TVP API (DB-API compatible with mssql-python, which has none); only the negative is tested |
| `test_info_messages.rs` | P `test_print_and_low_severity_raiserror_are_messages`, `test_info_messages_between_result_sets`; M `test_info_messages_land_in_cursor_messages`, `test_messages_reset_on_next_execute` | |
| `test_cursor_ops.rs`, `test_client_read_apis.rs`, `test_client_write_apis.rs`, `query_results.rs`, `test_large_value_read.rs` | P cursor-op tests, `test_large_*_read`; M `test_results.py` (custom result sets, chunked PLP, NULLs, Arrow) | |
| `test_client_transactions.rs`, `transaction.rs` | P `test_commit_rollback_and_savepoint`, `test_autocommit_toggle_and_trancount`, isolation-level test | |
| `test_connection_pool_primitives.rs` | E `tests/common_issues/test_rs_aio_pool_semantics.py`, `test_rs_core_regressions.py` (pooling), P reset tests | the Rust primitives themselves are internal |
| `test_tls_handshake.rs`, `test_mock_server_tls.rs`, `test_extended_protection.rs` | M `test_login_tls.py` (TDS 7.4 TLS, strict TDS 8, chain and hostname validation against a private CA, untrusted/mismatching certificate rejected) | extended protection (channel binding) needs Windows integrated auth: skipped |
| `test_encrypt_for_connections.rs` | P `test_encrypt_keyword_variants_connect_and_report_encryption[*]`; M `test_encrypt_required_against_plaintext_server_fails`, `test_strict_against_non_tls_server_fails` | |
| `test_mock_server.rs`, `test_mock_server_fedauth.rs` | M `test_login_tls.py`, `test_results.py`, `test_faults.py` | |
| `test_redirection.rs` | M `test_faults.py::test_plain_login_redirection`, `test_enhanced_routing_redirects_and_sets_database`, `test_redirection_to_dead_port_fails_cleanly` | malformed-routing variants (route before LOGINACK, duplicate routes) are not exposed by the harness |
| `test_login_server_name.rs` | M `test_database_and_server_name_in_login` | |
| `connectivity.rs`, `test_transport_protocols.rs`, `test_no_protocol_resolution.rs` | P `test_connect_to_unreachable_port_fails_fast`, `test_wrong_password_is_login_failed_error`; M `test_server_gone_before_connect` | named pipes / shared memory / LocalDB are Windows-only protocols arrowsql does not expose |
| `test_multi_subnet_failover.rs`, `test_ssrp_local.rs` | skipped | MultiSubnetFailover and SSRP (UDP 1434 instance resolution) are not implemented in the arrowsql connection layer |
| `test_always_encrypted.rs`, `test_udt_parameters.rs`, `test_data_classification.rs`, `test_vector_type.rs` | skipped | need a column master key store / CLR assemblies / classification-enabled server / SQL Server 2025; data classification tokens are not surfaced through DB-API |
| `test_kerberos_gssapi.rs`, `test_windows_sspi.rs` | skipped | integrated authentication needs a KDC or a Windows domain |
| `test_shared_single_worker_runtime.rs` | skipped | internal tokio runtime topology |

## Bugs found by the port

- `Cursor.bulkcopy` rejected a Python list of floats for a `vector` column (`Type mismatch ... target SQL
  type is Vector`). Fixed in `src/params/py_types.rs`; regression test:
  `test_vector_column_is_either_loaded_or_rejected_cleanly` (runs where the server has `vector`, found
  by the windows-db CI job).
- `Cursor.bulkcopy` rejected a Python `str` bound for an `xml` column
  (`Type mismatch for column 'v': converted to String(...) but target SQL type is Xml`). Fixed in
  `src/params/py_types.rs` (`py_to_column_value_internal` now produces `Xml` / `Json` values for
  those targets). Regression test: `test_bulkcopy_xml_round_trip`.

## Mock TDS server limits that shaped the tests

- Requests larger than the negotiated 4096-byte packet are not reassembled by the mock, and
  responses must fit one packet; PLP fixtures are kept small.
- The mock declares every column NOT NULL and can encode NULL only for integers and
  `nvarchar(max)`. NULL in a non-nullable Arrow column is therefore tested as an error.
- An unregistered batch gets a bare DONE (no result set).
