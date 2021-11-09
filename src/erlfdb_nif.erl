% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(erlfdb_nif).

-compile(no_native).
-on_load(init/0).

-export([
    ohai/0,

    get_max_api_version/0,

    future_cancel/1,
    future_silence/1,
    future_is_ready/1,
    future_get_error/1,
    future_get/1,

    create_database/1,
    database_set_option/2,
    database_set_option/3,
    database_create_transaction/1,

    transaction_set_option/2,
    transaction_set_option/3,
    transaction_set_read_version/2,
    transaction_get_read_version/1,
    transaction_get/3,
    transaction_get_key/3,
    transaction_get_addresses_for_key/2,
    transaction_get_range/9,
    transaction_set/3,
    transaction_clear/2,
    transaction_clear_range/3,
    transaction_atomic_op/4,
    transaction_commit/1,
    transaction_get_committed_version/1,
    transaction_get_versionstamp/1,
    transaction_watch/2,
    transaction_on_error/2,
    transaction_reset/1,
    transaction_cancel/1,
    transaction_add_conflict_range/4,
    transaction_get_next_tx_id/1,
    transaction_is_read_only/1,
    transaction_has_watches/1,
    transaction_get_writes_allowed/1,
    transaction_get_approximate_size/1,

    get_error/1,
    error_predicate/2
]).

-define(DEFAULT_API_VERSION, 620).

-type error() :: {erlfdb_error, Code :: integer()}.
-type future() :: {erlfdb_future, reference(), reference()}.
-type database() :: {erlfdb_database, reference()}.
-type transaction() :: {erlfdb_transaction, reference()}.

-type option_value() :: integer() | binary().

-type key_selector() ::
    {Key :: binary(), lt | lteq | gt | gteq}
    | {Key :: binary(), OrEqual :: boolean(), Offset :: integer()}.

-type future_result() ::
    database()
    | integer()
    | binary()
    | {[{binary(), binary()}], integer(), boolean()}
    | not_found
    | {error, invalid_future_type}.

-type network_option() ::
    local_address
    | cluster_file
    | trace_enable
    | trace_format
    | trace_roll_size
    | trace_max_logs_size
    | trace_log_group
    | knob
    | tls_plugin
    | tls_cert_bytes
    | tls_cert_path
    | tls_key_bytes
    | tls_key_path
    | tls_verify_peers
    | client_buggify_enable
    | client_buggify_disable
    | client_buggify_section_activated_probability
    | client_buggify_section_fired_probability
    | tls_ca_bytes
    | tls_ca_path
    | tls_password
    | disable_multi_version_client_api
    | callbacks_on_external_threads
    | external_client_library
    | external_client_directory
    | disable_local_client
    | disable_client_statistics_logging
    | enable_slow_task_profiling.

-type database_option() ::
    location_cache_size
    | max_watches
    | machine_id
    | datacenter_id.

-type transaction_option() ::
    causal_write_risky
    | causal_read_risky
    | causal_read_disable
    | next_write_no_write_conflict_range
    | read_your_writes_disable
    | read_ahead_disable
    | durability_datacenter
    | durability_risky
    | durability_dev_null_is_web_scale
    | priority_system_immediate
    | priority_batch
    | initialize_new_database
    | access_system_keys
    | read_system_keys
    | debug_retry_logging
    | transaction_logging_enable
    | timeout
    | retry_limit
    | max_retry_delay
    | snapshot_ryw_enable
    | snapshot_ryw_disable
    | lock_aware
    | used_during_commit_protection_disable
    | read_lock_aware
    | size_limit
    | allow_writes
    | disallow_writes.

-type streaming_mode() ::
    stream_want_all
    | stream_iterator
    | stream_exact
    | stream_small
    | stream_medium
    | stream_large
    | stream_serial.

-type atomic_mode() ::
    add
    | bit_and
    | bit_or
    | bit_xor
    | append_if_fits
    | max
    | min
    | byte_min
    | byte_max
    | set_versionstamped_key
    | set_versionstamped_value.

-type atomic_operand() :: integer() | binary().

-type conflict_type() :: read | write.

-type error_predicate() ::
    retryable
    | maybe_committed
    | retryable_not_committed.

ohai() ->
    foo.

-spec get_max_api_version() -> {ok, integer()}.
get_max_api_version() ->
    erlfdb_get_max_api_version().

-spec future_cancel(future()) -> ok.
future_cancel({erlfdb_future, _Ref, Ft}) ->
    erlfdb_future_cancel(Ft).

-spec future_silence(future()) -> ok.
future_silence({erlfdb_future, _Ref, Ft}) ->
    erlfdb_future_silence(Ft).

-spec future_is_ready(future()) -> boolean().
future_is_ready({erlfdb_future, _Ref, Ft}) ->
    erlfdb_future_is_ready(Ft).

-spec future_get_error(future()) -> error().
future_get_error({erlfdb_future, _Ref, Ft}) ->
    erlfdb_future_get_error(Ft).

-spec future_get(future()) -> future_result().
future_get({erlfdb_future, _Ref, Ft}) ->
    erlfdb_future_get(Ft).

-spec create_database(ClusterFilePath :: binary()) -> database().
create_database(<<>>) ->
    create_database(<<0>>);
create_database(ClusterFilePath) ->
    Size = size(ClusterFilePath) - 1,
    % Make sure we pass a NULL-terminated string
    % to FoundationDB
    NifPath =
        case ClusterFilePath of
            <<_:Size/binary, 0>> ->
                ClusterFilePath;
            _ ->
                <<ClusterFilePath/binary, 0>>
        end,
    erlfdb_create_database(NifPath).

-spec database_set_option(database(), Option :: database_option()) -> ok.
database_set_option(Database, Option) ->
    database_set_option(Database, Option, <<>>).

-spec database_set_option(
    database(),
    Option :: database_option(),
    Value :: option_value()
) -> ok.
database_set_option({erlfdb_database, Db}, Opt, Val) ->
    BinVal = option_val_to_binary(Val),
    erlfdb_database_set_option(Db, Opt, BinVal).

-spec database_create_transaction(database()) -> transaction().
database_create_transaction({erlfdb_database, Db}) ->
    erlfdb_database_create_transaction(Db).

-spec transaction_set_option(transaction(), Option :: transaction_option()) -> ok.
transaction_set_option(Transaction, Option) ->
    transaction_set_option(Transaction, Option, <<>>).

-spec transaction_set_option(
    transaction(),
    Option :: transaction_option(),
    Value :: option_value()
) -> ok.
transaction_set_option({erlfdb_transaction, Tx}, Opt, Val) ->
    BinVal = option_val_to_binary(Val),
    erlfdb_transaction_set_option(Tx, Opt, BinVal).

-spec transaction_set_read_version(transaction(), Version :: integer()) -> ok.
transaction_set_read_version({erlfdb_transaction, Tx}, Version) ->
    erlfdb_transaction_set_read_version(Tx, Version).

-spec transaction_get_read_version(transaction()) -> future().
transaction_get_read_version({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_read_version(Tx).

-spec transaction_get(transaction(), Key :: binary(), Snapshot :: boolean()) ->
    future().
transaction_get({erlfdb_transaction, Tx}, Key, Snapshot) ->
    erlfdb_transaction_get(Tx, Key, Snapshot).

-spec transaction_get_key(
    transaction(),
    KeySelector :: key_selector(),
    Snapshot :: boolean()
) -> future().
transaction_get_key({erlfdb_transaction, Tx}, KeySelector, Snapshot) ->
    erlfdb_transaction_get_key(Tx, KeySelector, Snapshot).

-spec transaction_get_addresses_for_key(transaction(), Key :: binary()) ->
    future().
transaction_get_addresses_for_key({erlfdb_transaction, Tx}, Key) ->
    erlfdb_transaction_get_addresses_for_key(Tx, Key).

-spec transaction_get_range(
    transaction(),
    StartKeySelector :: key_selector(),
    EndKeySelector :: key_selector(),
    Limit :: non_neg_integer(),
    TargetBytes :: non_neg_integer(),
    StreamingMode :: streaming_mode(),
    Iteration :: non_neg_integer(),
    Snapshot :: boolean(),
    Reverse :: integer()
) -> future().
transaction_get_range(
    {erlfdb_transaction, Tx},
    StartKeySelector,
    EndKeySelector,
    Limit,
    TargetBytes,
    StreamingMode,
    Iteration,
    Snapshot,
    Reverse
) ->
    erlfdb_transaction_get_range(
        Tx,
        StartKeySelector,
        EndKeySelector,
        Limit,
        TargetBytes,
        StreamingMode,
        Iteration,
        Snapshot,
        Reverse
    ).

-spec transaction_set(transaction(), Key :: binary(), Val :: binary()) -> ok.
transaction_set({erlfdb_transaction, Tx}, Key, Val) ->
    erlfdb_transaction_set(Tx, Key, Val).

-spec transaction_clear(transaction(), Key :: binary()) -> ok.
transaction_clear({erlfdb_transaction, Tx}, Key) ->
    erlfdb_transaction_clear(Tx, Key).

-spec transaction_clear_range(
    transaction(),
    StartKey :: binary(),
    EndKey :: binary()
) -> ok.
transaction_clear_range({erlfdb_transaction, Tx}, StartKey, EndKey) ->
    erlfdb_transaction_clear_range(Tx, StartKey, EndKey).

-spec transaction_atomic_op(
    transaction(),
    Key :: binary(),
    Operand :: atomic_operand(),
    Mode :: atomic_mode()
) -> ok.
transaction_atomic_op({erlfdb_transaction, Tx}, Key, Operand, OpName) ->
    BinOperand =
        case Operand of
            Bin when is_binary(Bin) ->
                Bin;
            Int when is_integer(Int) ->
                <<Int:64/little>>
        end,
    erlfdb_transaction_atomic_op(Tx, Key, BinOperand, OpName).

-spec transaction_commit(transaction()) -> future().
transaction_commit({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_commit(Tx).

-spec transaction_get_committed_version(transaction()) -> integer().
transaction_get_committed_version({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_committed_version(Tx).

-spec transaction_get_versionstamp(transaction()) -> future().
transaction_get_versionstamp({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_versionstamp(Tx).

-spec transaction_watch(transaction(), Key :: binary()) -> future().
transaction_watch({erlfdb_transaction, Tx}, Key) ->
    erlfdb_transaction_watch(Tx, Key).

-spec transaction_on_error(transaction(), Error :: integer()) -> future().
transaction_on_error({erlfdb_transaction, Tx}, Error) ->
    erlfdb_transaction_on_error(Tx, Error).

-spec transaction_reset(transaction()) -> ok.
transaction_reset({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_reset(Tx).

-spec transaction_cancel(transaction()) -> ok.
transaction_cancel({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_cancel(Tx).

-spec transaction_add_conflict_range(
    transaction(),
    StartKey :: binary(),
    EndKey :: binary(),
    ConflictType :: conflict_type()
) -> ok.
transaction_add_conflict_range(
    {erlfdb_transaction, Tx},
    StartKey,
    EndKey,
    ConflictType
) ->
    erlfdb_transaction_add_conflict_range(Tx, StartKey, EndKey, ConflictType).

-spec transaction_get_approximate_size(transaction()) -> non_neg_integer().
transaction_get_approximate_size({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_approximate_size(Tx).

-spec transaction_get_next_tx_id(transaction()) -> non_neg_integer().
transaction_get_next_tx_id({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_next_tx_id(Tx).

-spec transaction_is_read_only(transaction()) -> true | false.
transaction_is_read_only({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_is_read_only(Tx).

-spec transaction_has_watches(transaction()) -> true | false.
transaction_has_watches({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_has_watches(Tx).

-spec transaction_get_writes_allowed(transaction()) -> true | false.
transaction_get_writes_allowed({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_writes_allowed(Tx).

-spec get_error(integer()) -> binary().
get_error(Error) ->
    erlfdb_get_error(Error).

-spec error_predicate(Predicate :: error_predicate(), Error :: integer()) ->
    boolean().
error_predicate(Predicate, Error) ->
    erlfdb_error_predicate(Predicate, Error).

-spec option_val_to_binary(binary() | integer()) -> binary().
option_val_to_binary(Val) when is_binary(Val) ->
    Val;
option_val_to_binary(Val) when is_integer(Val) ->
    <<Val:8/little-unsigned-integer-unit:8>>.

init() ->
    PrivDir =
        case code:priv_dir(?MODULE) of
            {error, _} ->
                EbinDir = filename:dirname(code:which(?MODULE)),
                AppPath = filename:dirname(EbinDir),
                filename:join(AppPath, "priv");
            Path ->
                Path
        end,
    Status = erlang:load_nif(filename:join(PrivDir, "erlfdb_nif"), 0),
    if
        Status /= ok ->
            Status;
        true ->
            true = erlfdb_can_initialize(),

            Vsn =
                case application:get_env(erlfdb, api_version) of
                    {ok, V} -> V;
                    undefined -> ?DEFAULT_API_VERSION
                end,
            ok = select_api_version(Vsn),

            Opts =
                case application:get_env(erlfdb, network_options) of
                    {ok, O} when is_list(O) -> O;
                    undefined -> []
                end,

            lists:foreach(
                fun(Option) ->
                    case Option of
                        Name when is_atom(Name) ->
                            ok = network_set_option(Name, <<>>);
                        {Name, Value} when is_atom(Name) ->
                            ok = network_set_option(Name, Value)
                    end
                end,
                Opts
            ),

            ok = erlfdb_setup_network()
    end.

-define(NOT_LOADED, erlang:nif_error({erlfdb_nif_not_loaded, ?FILE, ?LINE})).

-spec select_api_version(Version :: pos_integer()) -> ok.
select_api_version(Version) when is_integer(Version), Version > 0 ->
    erlfdb_select_api_version(Version).

-spec network_set_option(Option :: network_option(), Value :: option_value()) ->
    ok | error().
network_set_option(Name, Value) ->
    BinValue =
        case Value of
            B when is_binary(B) -> B;
            I when is_integer(I) -> <<I:8/little-unsigned-integer-unit:8>>
        end,
    erlfdb_network_set_option(Name, BinValue).

% Sentinel Check
erlfdb_can_initialize() -> ?NOT_LOADED.

% Versioning
erlfdb_get_max_api_version() -> ?NOT_LOADED.
erlfdb_select_api_version(_Version) -> ?NOT_LOADED.

% Networking Setup
erlfdb_network_set_option(_NetworkOption, _Value) -> ?NOT_LOADED.
erlfdb_setup_network() -> ?NOT_LOADED.

% Futures
erlfdb_future_cancel(_Future) -> ?NOT_LOADED.
erlfdb_future_silence(_Future) -> ?NOT_LOADED.
erlfdb_future_is_ready(_Future) -> ?NOT_LOADED.
erlfdb_future_get_error(_Future) -> ?NOT_LOADED.
erlfdb_future_get(_Future) -> ?NOT_LOADED.

% Databases
erlfdb_create_database(_ClusterFilePath) -> ?NOT_LOADED.
erlfdb_database_set_option(_Database, _DatabaseOption, _Value) -> ?NOT_LOADED.
erlfdb_database_create_transaction(_Database) -> ?NOT_LOADED.

% Transactions
erlfdb_transaction_set_option(_Transaction, _TransactionOption, _Value) -> ?NOT_LOADED.
erlfdb_transaction_set_read_version(_Transaction, _Version) -> ?NOT_LOADED.
erlfdb_transaction_get_read_version(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_get(_Transaction, _Key, _Snapshot) -> ?NOT_LOADED.
erlfdb_transaction_get_key(_Transaction, _KeySelector, _Snapshot) -> ?NOT_LOADED.
erlfdb_transaction_get_addresses_for_key(_Transaction, _Key) -> ?NOT_LOADED.
erlfdb_transaction_get_range(
    _Transaction,
    _StartKeySelector,
    _EndKeySelector,
    _Limit,
    _TargetBytes,
    _StreamingMode,
    _Iteration,
    _Snapshot,
    _Reverse
) ->
    ?NOT_LOADED.
erlfdb_transaction_set(_Transaction, _Key, _Value) -> ?NOT_LOADED.
erlfdb_transaction_clear(_Transaction, _Key) -> ?NOT_LOADED.
erlfdb_transaction_clear_range(_Transaction, _StartKey, _EndKey) -> ?NOT_LOADED.
erlfdb_transaction_atomic_op(_Transaction, _Mutation, _Key, _Value) -> ?NOT_LOADED.
erlfdb_transaction_commit(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_get_committed_version(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_get_versionstamp(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_watch(_Transaction, _Key) -> ?NOT_LOADED.
erlfdb_transaction_on_error(_Transaction, _Error) -> ?NOT_LOADED.
erlfdb_transaction_reset(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_cancel(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_add_conflict_range(_Transaction, _StartKey, _EndKey, _Type) -> ?NOT_LOADED.
erlfdb_transaction_get_next_tx_id(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_is_read_only(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_has_watches(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_get_writes_allowed(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_get_approximate_size(_Transaction) -> ?NOT_LOADED.

% Misc
erlfdb_get_error(_Error) -> ?NOT_LOADED.
erlfdb_error_predicate(_Predicate, _Error) -> ?NOT_LOADED.
