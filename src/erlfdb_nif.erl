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
    ohai/0
]).


ohai() ->
    foo.


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
        {ok, Vsn} = erlfdb_get_max_api_version(),
        erlfdb_select_api_version(Vsn)
    end.


-define(NOT_LOADED, erlang:nif_error({erlfdb_nif_not_loaded, ?FILE, ?LINE})).


% Versioning
erlfdb_get_max_api_version() -> ?NOT_LOADED.
erlfdb_select_api_version(_Version) -> ?NOT_LOADED.

%% % Networking Setup
%% erlfdb_network_set_option(_NetworkOption, _Value) -> ?NOT_LOADED.
%% erlfdb_setup_network() -> ?NOT_LOADED.
%%
%% % Futures
%% erlfdb_future_cancel(_Future) -> ?NOT_LOADED.
%% erlfdb_future_is_ready(_Future) -> ?NOT_LOADED.
%% erlfdb_future_get_error(_Future) -> ?NOT_LOADED.
%% erlfdb_future_get_version(_Future) -> ?NOT_LOADED.
%% erlfdb_future_get(_Future) -> ?NOT_LOADED.
%%
%% % Clusters
%% erlfdb_create_cluster() -> ?NOT_LOADED.
%% erlfdb_create_cluster(_ClusterFile) -> ?NOT_LOADED.
%% erlfdb_cluster_set_option(_Cluster, _ClusterOption) -> ?NOT_LOADED.
%% erlfdb_cluster_set_option(_Cluster, _ClusterOption, _Value) -> ?NOT_LOADED.
%% erlfdb_cluster_create_database(_Cluster, _DbName) -> ?NOT_LOADED.
%%
%% % Databases
%% erlfdb_database_set_option(_Database, _DatabaseOption) -> ?NOT_LOADED.
%% erlfdb_database_set_option(_Database, _DatabaseOption, _Value) -> ?NOT_LOADED.
%% erlfdb_database_create_transaction(_Database) -> ?NOT_LOADED.
%%
%% % Transactions
%% erlfdb_transaction_set_option(_Transaction, _TransactionOption) -> ?NOT_LOADED.
%% erlfdb_transaction_set_option(_Transaction, _TransactionOption, _Value) -> ?NOT_LOADED.
%% erlfdb_transaction_set_read_version(_Transaction, _Version) -> ?NOT_LOADED.
%% erlfdb_transaction_get_read_version(_Transaction) -> ?NOT_LOADED.
%% erlfdb_transaction_get(_Transaction, _Key) -> ?NOT_LOADED.
%% erlfdb_transaction_get(_Transaction, _Key, _Snapshot) -> ?NOT_LOADED.
%% erlfdb_transaction_get_key(_Transaction, _Key) -> ?NOT_LOADED.
%% erlfdb_transaction_get_range(_Transaction, _Range, _GetRangeOptions) -> ?NOT_LOADED.
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