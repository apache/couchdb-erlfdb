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

-module(erlfdb).


-compile({no_auto_import, [get/1]}).


-export([
    open/0,
    open/1,

    create_transaction/1,
    transactional/2,

    % Db/Tx configuration
    set_option/2,
    set_option/3,

    % Lifecycle Management
    commit/1,
    reset/1,
    cancel/1,

    % Future Specific functions
    is_ready/1,
    get/1,
    get_error/1,
    block_until_ready/1,
    wait/1,
    wait/2,
    wait_for_any/1,
    wait_for_any/2,
    wait_for_all/1,
    wait_for_all/2,

    % Data retrieval
    get/2,
    get_key/2,
    get_range/3,
    get_range/4,
    get_range_startswith/2,
    get_range_startswith/3,

    % Snapshot reads
    get_ss/2,
    get_key_ss/2,
    get_range_ss/3,
    get_range_ss/4,
    get_range_startswith_ss/2,
    get_range_startswith_ss/3,

    % Data modifications
    set/3,
    clear/2,
    clear_range/3,
    clear_range_startswith/2,

    % Atomic operations
    add/3,
    bit_and/3,
    bit_or/3,
    bit_xor/3,
    min/3,
    max/3,
    byte_min/3,
    byte_max/3,
    set_versionstamped_key/3,
    set_versionstamped_value/3,
    atomic_op/4,

    % Watches
    watch/2,
    get_and_watch/2,
    set_and_watch/3,
    clear_and_watch/2,

    % Conflict ranges
    add_read_conflict_key/2,
    add_read_conflict_range/3,
    add_write_conflict_key/2,
    add_write_conflict_range/3,
    add_conflict_range/4,

    % Transaction versioning
    set_read_version/2,
    get_read_version/1,
    get_committed_version/1,
    get_versionstamp/1,

    % Locality
    get_addresses_for_key/2,

    % Misc
    on_error/2,
    error_predicate/2
]).


-define(IS_FUTURE, {erlfdb_future, _, _}).
-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).
-define(IS_SS, {erlfdb_snapshot, _}).


open() ->
    open(<<>>).


open(ClusterFile) ->
    Cluster = wait(erlfdb_nif:create_cluster(ClusterFile)),
    wait(erlfdb_nif:cluster_create_database(Cluster, <<"DB">>)).


create_transaction(?IS_DB = Db) ->
    {erlfdb_database, DbRef} = Db,
    erlfdb_nif:database_create_transaction(DbRef).


transactional(?IS_DB = Db, UserFun) when is_function(UserFun, 1) ->
    Tx = create_transaction(Db),
    try
        Ret = UserFun(Tx),
        wait(commit(Tx)),
        Ret
    catch error:{erlfdb_error, Code} ->
        wait(on_error(Tx, Code)),
        transactional(Db, UserFun)
    end.


set_option(DbOrTx, Option) ->
    set_option(DbOrTx, Option, <<>>).


set_option(?IS_DB = Db, DbOption, Value) ->
    {erlfdb_database, DbRef} = Db,
    erlfdb_nif:database_set_option(DbRef, DbOption, Value);

set_option(?IS_TX = Tx, TxOption, Value) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_set_option(TxRef, TxOption, Value).


commit(?IS_TX = Tx) ->
    wait(erlfdb_nif:transaction_commit(Tx)).


reset(?IS_TX = Tx) ->
    ok = erlfdb_nif:transaction_reset(Tx).


cancel(?IS_FUTURE = Future) ->
    ok = erlfdb_nif:future_cancel(Future);

cancel(?IS_TX = Tx) ->
    ok = erlfdb_nif:transaction_cancel(Tx).


is_ready(?IS_FUTURE = Future) ->
    {erlfdb_future, _MsgRef, FRef} = Future,
    erlfdb_nif:future_is_ready(FRef).


get_error(?IS_FUTURE = Future) ->
    {erlfdb_future, _MsgRef, FRef} = Future,
    erlfdb_nif:future_get_error(FRef).


get(?IS_FUTURE = Future) ->
    {erlfdb_future, _MsgRef, FRef} = Future,
    erlfdb_nif:future_get(FRef).


block_until_ready(?IS_FUTURE = Future) ->
    {erlfdb_future, MsgRef, _FRef} = Future,
    receive
        {MsgRef, ready} -> ok
    end.


wait(?IS_FUTURE = Future) ->
    wait(Future, []).

wait(?IS_FUTURE = Future, Options) ->
    Timeout = get_value(Options, timeout, 5000),
    {erlfdb_future, MsgRef, FRef} = Future,
    receive
        {MsgRef, ready} -> get(FRef)
    after Timeout ->
        erlang:error({timeout, Future})
    end.


wait_for_any(Futures) ->
    wait_for_any(Futures, []).


wait_for_any(Futures, Options) ->
    wait_for_any(Futures, Options, []).


wait_for_any(Futures, Options, ResendQ) ->
    Timeout = get_value(Options, timeout, 5000),
    receive
        {MsgRef, ready} = Msg ->
            case lists:keyfind(MsgRef, 2, Futures) of
                ?IS_FUTURE = Future ->
                    lists:foreach(fun(M) ->
                        self() ! M
                    end, ResendQ),
                    Future;
                _ ->
                    wait_for_any(Futures, Options, [Msg | ResendQ])
            end
    after Timeout ->
        lists:foreach(fun(M) ->
            self() ! M
        end, ResendQ),
        erlang:error({timeout, Futures})
    end.


wait_for_all(Futures) ->
    wait_for_all(Futures, []).


wait_for_all(Futures, Options) ->
    % Same as wait for all. We might want to
    % handle timeouts here so we have a single
    % timeout for all future waiting.
    lists:map(fun(Future) ->
        wait(Future, Options)
    end, Futures).


get(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        wait(get(Tx, Key))
    end);

get(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get(TxRef, Key, false).


get_key(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        wait(get_key(Tx, Key))
    end);

get_key(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get_key(TxRef, Key, false).


get_range(DbOrTx, StartKey, EndKey) ->
    get_range(DbOrTx, StartKey, EndKey, []).


get_range(?IS_DB = Db, StartKey, EndKey, Options) ->
    transactional(Db, fun(Tx) ->
        wait(get_range(Tx, StartKey, EndKey, Options))
    end);

get_range(?IS_TX = Tx, StartKey, EndKey, Options) ->
    {erlfdb_transaction, TxRef} = Tx,
    SKSelector = erlfdb_key:to_selector(StartKey),
    EKSelector = erlfdb_key:to_selector(EndKey),
    Limit = get_value(Options, limit, 0),
    TargetBytes = get_value(Options, target_bytes, 0),
    StreamingMode = get_value(Options, streaming_mode, want_all),
    Iteration = get_value(Options, iteration, 1),
    Snapshot = get_value(Options, snapshot, false),
    ReverseBool = get_value(Options, reverse, false),
    Reverse = if ReverseBool -> 1; true -> 0 end,
    erlfdb_nif:transaction_get_range(
            TxRef,
            SKSelector,
            EKSelector,
            Limit,
            TargetBytes,
            StreamingMode,
            Iteration,
            Snapshot,
            Reverse
        ).


get_range_startswith(DbOrTx, Prefix) ->
    get_range_startswith(DbOrTx, Prefix, []).


get_range_startswith(DbOrTx, Prefix, Options) ->
    SKey = Prefix,
    EKey = erlfdb_key:strinc(Prefix),
    get_range(DbOrTx, SKey, EKey, Options).


get_ss(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get(TxRef, Key, true).


get_key_ss(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get_key(TxRef, Key, true).


get_range_ss(?IS_TX = Tx, StartKey, EndKey) ->
    get_range(Tx, StartKey, EndKey, [{snapshot, true}]).


get_range_ss(?IS_TX = Tx, StartKey, EndKey, Options) ->
    get_range(Tx, StartKey, EndKey, [{snapshot, true} | Options]).


get_range_startswith_ss(?IS_TX = Tx, Prefix) ->
    get_range_startswith(Tx, Prefix, [{snapshot, true}]).


get_range_startswith_ss(?IS_TX = Tx, Prefix, Options) ->
    get_range_startswith(Tx, Prefix, [{snapshot, true} | Options]).


set(?IS_DB = Db, Key, Value) ->
    transactional(Db, fun(Tx) ->
        set(Tx, Key, Value)
    end);

set(?IS_TX = Tx, Key, Value) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_set(TxRef, Key, Value).


clear(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        clear(Tx, Key)
    end);

clear(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_clear(TxRef, Key).


clear_range(?IS_DB = Db, StartKey, EndKey) ->
    transactional(Db, fun(Tx) ->
        clear_range(Tx, StartKey, EndKey)
    end);

clear_range(?IS_TX = Tx, StartKey, EndKey) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_clear_range(TxRef, StartKey, EndKey).


clear_range_startswith(?IS_DB = Db, Prefix) ->
    transactional(Db, fun(Tx) ->
        clear_range_startswith(Tx, Prefix)
    end);

clear_range_startswith(?IS_TX = Tx, Prefix) ->
    {erlfdb_transaction, TxRef} = Tx,
    EndKey = erlfdb_key:strinc(Prefix),
    erlfdb_nif:transaction_clear_range(TxRef, Prefix, EndKey).


add(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, add).


bit_and(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, bit_and).


bit_or(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, bit_or).


bit_xor(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, bit_xor).


min(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, min).


max(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, max).


byte_min(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, byte_min).


byte_max(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, byte_max).


set_versionstamped_key(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, set_versionstamped_key).


set_versionstamped_value(DbOrTx, Key, Param) ->
    atomic_op(DbOrTx, Key, Param, set_versionstamped_value).


atomic_op(?IS_DB = Db, Key, Param, Op) ->
    transactional(Db, fun(Tx) ->
        atomic_op(Tx, Key, Param, Op)
    end);

atomic_op(?IS_TX = Tx, Key, Param, Op) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_atomic_op(TxRef, Key, Param, Op).


watch(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        watch(Tx, Key)
    end);

watch(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_watch(TxRef, Key).


get_and_watch(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        KeyFuture = get(Tx, Key),
        WatchFuture = watch(Tx, Key),
        {wait(KeyFuture), WatchFuture}
    end).


set_and_watch(?IS_DB = Db, Key, Value) ->
    transactional(Db, fun(Tx) ->
        set(Tx, Key, Value),
        watch(Tx, Key)
    end).


clear_and_watch(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        clear(Tx, Key),
        watch(Tx, Key)
    end).


add_read_conflict_key(?IS_TX = Tx, Key) ->
    add_read_conflict_range(Tx, Key, <<Key/binary, 16#00>>).


add_read_conflict_range(?IS_TX = Tx, StartKey, EndKey) ->
    add_conflict_range(Tx, StartKey, EndKey, read).


add_write_conflict_key(?IS_TX = Tx, Key) ->
    add_write_conflict_range(Tx, Key, <<Key/binary, 16#00>>).


add_write_conflict_range(?IS_TX = Tx, StartKey, EndKey) ->
    add_conflict_range(Tx, StartKey, EndKey, write).


add_conflict_range(?IS_TX = Tx, StartKey, EndKey, Type) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_add_conflict_range(TxRef, StartKey, EndKey, Type).


set_read_version(?IS_TX = Tx, Version) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_set_read_version(TxRef, Version).


get_read_version(?IS_TX = Tx) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get_read_version(TxRef).


get_committed_version(?IS_TX = Tx) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get_committed_version(TxRef).


get_versionstamp(?IS_TX = Tx) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get_versionstamp(TxRef).


get_addresses_for_key(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        wait(get_addresses_for_key(Tx, Key))
    end);

get_addresses_for_key(?IS_TX = Tx, Key) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_get_addresses_for_key(TxRef, Key).


on_error(?IS_TX = Tx, {erlfdb_error, ErrorCode}) ->
    on_error(Tx, ErrorCode);

on_error(?IS_TX = Tx, ErrorCode) ->
    {erlfdb_transaction, TxRef} = Tx,
    erlfdb_nif:transaction_on_error(TxRef, ErrorCode).


error_predicate(Predicate, {erlfdb_error, ErrorCode}) ->
    error_predicate(Predicate, ErrorCode);

error_predicate(Predicate, ErrorCode) ->
    erlfdb_nif:error_predicate(Predicate, ErrorCode).


get_value(Options, Name, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {Name, Value} -> Value;
        _ -> Default
    end.
