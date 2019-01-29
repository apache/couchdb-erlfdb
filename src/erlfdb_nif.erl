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
    future_is_ready/1,
    future_get_error/1,
    future_get/1,

    create_cluster/0,
    create_cluster/1,
    cluster_set_option/3,
    cluster_create_database/2,

    database_set_option/3,
    database_create_transaction/1,

    transaction_set_option/3,
    transaction_set_read_version/2,
    transaction_get_read_version/1,
    transaction_get/2,
    transaction_get/3,
    transaction_get_key/3,
    transaction_get_addresses_for_key/2,
    transaction_get_range/9
]).


-define(DEFAULT_API_VERSION, 600).


-type future() :: {erlfdb_future, reference(), reference()}.
-type cluster() :: {erlfdb_cluster, reference()}.
-type database() :: {erlfdb_database, reference()}.
-type transaction() :: {erlfdb_transaction, reference()}.

-type error() :: {error, Code::integer(), Description::binary()}.
-type option_value() :: no_value | integer() | binary().

-type key_selector() ::
    {Key::binary(), lt | lteq | gt | gteq} |
    {Key::binary(), OrEqual::boolean(), Offset::integer()}.

-type future_result() ::
    {ok, cluster()} |
    {ok, database()} |
    {ok, Version::integer()} |
    {ok, KeyOrValue::binary()} |
    {ok, not_found} |
    {error, invalid_future_type} |
    error().

-type streaming_mode() ::
    stream_want_all |
    stream_iterator |
    stream_exact |
    stream_small |
    stream_medium |
    stream_large |
    stream_serial.


ohai() ->
    foo.


-spec get_max_api_version() -> {ok, integer()}.
get_max_api_version() ->
    erlfdb_get_max_api_version().


-spec future_cancel(future()) -> ok.
future_cancel({erlfdb, _Ref, Ft}) ->
    erlfdb_future_cancel(Ft).


-spec future_is_ready(future()) -> boolean().
future_is_ready({elfdb, _Ref, Ft}) ->
    erlfdb_future_is_ready(Ft).


-spec future_get_error(future()) -> error().
future_get_error({erlfdb, _Ref, Ft}) ->
    erlfdb_future_get_error(Ft).


-spec future_get(future()) -> future_result().
future_get({erlfdb, _Ref, Ft}) ->
    erlfdb_future_get(Ft).


-spec create_cluster() -> future().
create_cluster() ->
    create_cluster(<<0>>).


-spec create_cluster(ClusterFilePath::binary()) -> future().
create_cluster(<<>>) ->
    create_cluster(<<0>>);

create_cluster(ClusterFilePath) ->
    Size = size(ClusterFilePath) - 1,
    % Make sure we pass a NULL-terminated string
    % to FoundationDB
    NifPath = case ClusterFilePath of
        <<_:Size/binary, 0>> ->
            ClusterFilePath;
        _ ->
            <<ClusterFilePath/binary, 0>>
    end,
    erlfdb_create_cluster(NifPath).


-spec cluster_set_option(cluster(), Opt::atom(), Val::option_value()) ->
        ok | error().
cluster_set_option({erlfdb, Cluster}, Opt, Value) ->
    erlfdb_cluster_set_option(Cluster, Opt, Value).


-spec cluster_create_database(cluster(), DbName::binary()) ->
        {ok, database()} | error().
cluster_create_database({erlfdb_cluster, Cluster}, DbName) ->
    erlfdb_cluster_create_database(Cluster, DbName).


-spec database_set_option(database(), Opt::atom(), Val::option_value()) ->
        ok | error().
database_set_option({erlfdb, Db}, Opt, Val) ->
    erlfdb_database_set_option(Db, Opt, Val).


-spec database_create_transaction(database()) ->
        {ok, transaction()} | error().
database_create_transaction({erlfdb_database, Db}) ->
    erlfdb_database_create_transaction(Db).


-spec transaction_set_option(transaction(), Opt::atom(), Val::option_value()) ->
        ok | error().
transaction_set_option({erlfdb_transaction, Tx}, Opt, Val) ->
    erlfdb_transaction_set_option(Tx, Opt, Val).


-spec transaction_set_read_version(transaction(), Version::integer()) -> ok.
transaction_set_read_version({erlfdb_transaction, Tx}, Version) ->
    erlfdb_transaction_set_read_version(Tx, Version).


-spec transaction_get_read_version(transaction()) -> future() | error().
transaction_get_read_version({erlfdb_transaction, Tx}) ->
    erlfdb_transaction_get_read_version(Tx).


-spec transaction_get(transaction(), Key::binary()) -> future() | error().
transaction_get({erlfdb_transaction, Tx}, Key) ->
    erlfdb_transaction_get(Tx, Key, false).

-spec transaction_get(transaction(), Key::binary(), Snapshot::boolean()) ->
        future() | error().
transaction_get({erlfdb_transaction, Tx}, Key, Snapshot) ->
    erlfdb_transaction_get(Tx, Key, Snapshot).


-spec transaction_get_key(
        transaction(),
        KeySelector::key_selector(),
        Snapshot::boolean()
    ) -> future() | error().
transaction_get_key({erlfdb_transaction, Tx}, KeySelector, Snapshot) ->
    erlfdb_transaction_get_key(Tx, KeySelector, Snapshot).


-spec transaction_get_addresses_for_key(transaction(), Key::binary()) ->
        future() | error().
transaction_get_addresses_for_key({erlfdb_transaction, Tx}, Key) ->
    erlfdb_transaction_get_addresses_for_key(Tx, Key).


-spec transaction_get_range(
        transaction(),
        StartKeySelector::key_selector(),
        EndKeySelector::key_selector(),
        Limit::non_neg_integer(),
        TargetBytes::non_neg_integer(),
        StreamingMode::streaming_mode(),
        Iteration::non_neg_integer(),
        Snapshot::boolean(),
        Reverse::boolean()
    ) -> future() | error().
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


init() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end,
    Status = erlang:load_nif(filename:join(PrivDir, "erlfdb_nif"), 0),
    if Status /= ok -> Status; true ->
        true = erlfdb_can_initialize(),

        Vsn = case application:get_env(erlfdb, api_version) of
            {ok, V} -> V;
            undefined -> ?DEFAULT_API_VERSION
        end,
        ok = erlfdb_select_api_version(Vsn),

        Opts = case application:get_env(erlfdb, network_options) of
            {ok, O} when is_list(O) -> O;
            undefined -> []
        end,

        lists:foreach(fun({Name, Value}) ->
            ok = erlfdb_network_set_option(Name, Value)
        end, Opts),

        erlfdb_setup_network()
    end.


-define(NOT_LOADED, erlang:nif_error({erlfdb_nif_not_loaded, ?FILE, ?LINE})).


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
erlfdb_future_is_ready(_Future) -> ?NOT_LOADED.
erlfdb_future_get_error(_Future) -> ?NOT_LOADED.
erlfdb_future_get(_Future) -> ?NOT_LOADED.

% Clusters
erlfdb_create_cluster(_ClusterFile) -> ?NOT_LOADED.
erlfdb_cluster_set_option(_Cluster, _ClusterOption, _Value) -> ?NOT_LOADED.
erlfdb_cluster_create_database(_Cluster, _DbName) -> ?NOT_LOADED.

% Databases
erlfdb_database_set_option(_Database, _DatabaseOption, _Value) -> ?NOT_LOADED.
erlfdb_database_create_transaction(_Database) -> ?NOT_LOADED.


% Transactions
erlfdb_transaction_set_option(
        _Transaction,
        _TransactionOption,
        _Value
    ) -> ?NOT_LOADED.
erlfdb_transaction_set_read_version(_Transaction, _Version) -> ?NOT_LOADED.
erlfdb_transaction_get_read_version(_Transaction) -> ?NOT_LOADED.
erlfdb_transaction_get(_Transaction, _Key, _Snapshot) -> ?NOT_LOADED.
erlfdb_transaction_get_key(
        _Transaction,
        _KeySelector,
        _Snapshot
    ) -> ?NOT_LOADED.
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
    ) -> ?NOT_LOADED.
%% erlfdb_transaction_set(_Transaction, _Key, _Value) -> ?NOT_LOADED.
%% erlfdb_transaction_clear(_Transaction, _Key) -> ?NOT_LOADED.
%% erlfdb_transaction_clear_range(_Transaction, _StartKey, _EndKey) -> ?NOT_LOADED.
%% erlfdb_transaction_atomic_op(_Transaction, _Mutation, _Key, _Value) -> ?NOT_LOADED.
%% erlfdb_transaction_commit(_Transaction) -> ?NOT_LOADED.
%% erlfdb_transaction_get_committed_version(_Transaction) -> ?NOT_LOADED.
%% erlfdb_transaction_get_versionstamp(_Transaction) -> ?NOT_LOADED.
%% erlfdb_transaction_watch(_Transaction, _Key) -> ?NOT_LOADED.
%% erlfdb_transaction_on_eror(_Transaction, _Error) -> ?NOT_LOADED.
%% erlfdb_transaction_reset(_Transaction) -> ?NOT_LOADED.
%% erlfdb_transaction_cancel(_Transaction) -> ?NOT_LOADED.
%% erlfdb_transaction_add_conflict_range(_Transaction, _StartKey, _EndKey, _Type) -> ?NOT_LOADED.
%%
%% % Misc
%% erlfdb_get_error(_Error) -> ?NOT_LOADED.
%% erlfdb_error_predicate(_Predicate, _Error) -> ?NOT_LOADED.