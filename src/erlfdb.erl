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
    snapshot/1,

    % Db/Tx configuration
    set_option/2,
    set_option/3,

    % Lifecycle Management
    commit/1,
    reset/1,
    cancel/1,
    cancel/2,

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
    get_ss/2,

    get_key/2,
    get_key_ss/2,

    get_range/3,
    get_range/4,

    get_range_startswith/2,
    get_range_startswith/3,

    fold_range/5,
    fold_range/6,

    fold_range_future/4,
    fold_range_wait/4,

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

    % Transaction size info
    get_approximate_size/1,

    % Transaction status
    get_next_tx_id/1,
    is_read_only/1,
    has_watches/1,
    get_writes_allowed/1,

    % Locality
    get_addresses_for_key/2,

    % Get conflict information
    get_conflicting_keys/1,

    % Misc
    on_error/2,
    error_predicate/2,
    get_last_error/0,
    get_error_string/1
]).

-define(IS_FUTURE, {erlfdb_future, _, _}).
-define(IS_FOLD_FUTURE, {fold_info, _, _}).
-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).
-define(IS_SS, {erlfdb_snapshot, _}).
-define(GET_TX(SS), element(2, SS)).
-define(ERLFDB_ERROR, '$erlfdb_error').

-record(fold_st, {
    start_key,
    end_key,
    limit,
    target_bytes,
    streaming_mode,
    iteration,
    snapshot,
    reverse
}).

open() ->
    open(<<>>).

open(ClusterFile) ->
    erlfdb_nif:create_database(ClusterFile).

create_transaction(?IS_DB = Db) ->
    erlfdb_nif:database_create_transaction(Db).

transactional(?IS_DB = Db, UserFun) when is_function(UserFun, 1) ->
    clear_erlfdb_error(),
    Tx = create_transaction(Db),
    do_transaction(Tx, UserFun);
transactional(?IS_TX = Tx, UserFun) when is_function(UserFun, 1) ->
    UserFun(Tx);
transactional(?IS_SS = SS, UserFun) when is_function(UserFun, 1) ->
    UserFun(SS).

snapshot(?IS_TX = Tx) ->
    {erlfdb_snapshot, Tx};
snapshot(?IS_SS = SS) ->
    SS.

set_option(DbOrTx, Option) ->
    set_option(DbOrTx, Option, <<>>).

set_option(?IS_DB = Db, DbOption, Value) ->
    erlfdb_nif:database_set_option(Db, DbOption, Value);
set_option(?IS_TX = Tx, TxOption, Value) ->
    erlfdb_nif:transaction_set_option(Tx, TxOption, Value).

commit(?IS_TX = Tx) ->
    erlfdb_nif:transaction_commit(Tx).

reset(?IS_TX = Tx) ->
    ok = erlfdb_nif:transaction_reset(Tx).

cancel(?IS_FOLD_FUTURE = FoldInfo) ->
    cancel(FoldInfo, []);
cancel(?IS_FUTURE = Future) ->
    cancel(Future, []);
cancel(?IS_TX = Tx) ->
    ok = erlfdb_nif:transaction_cancel(Tx).

cancel(?IS_FOLD_FUTURE = FoldInfo, Options) ->
    {fold_info, _St, Future} = FoldInfo,
    cancel(Future, Options);
cancel(?IS_FUTURE = Future, Options) ->
    ok = erlfdb_nif:future_cancel(Future),
    case erlfdb_util:get(Options, flush, false) of
        true -> flush_future_message(Future);
        false -> ok
    end.

is_ready(?IS_FUTURE = Future) ->
    erlfdb_nif:future_is_ready(Future).

get_error(?IS_FUTURE = Future) ->
    erlfdb_nif:future_get_error(Future).

get(?IS_FUTURE = Future) ->
    erlfdb_nif:future_get(Future).

block_until_ready(?IS_FUTURE = Future) ->
    {erlfdb_future, MsgRef, _FRef} = Future,
    receive
        {MsgRef, ready} -> ok
    end.

wait(?IS_FUTURE = Future) ->
    wait(Future, []);
wait(Ready) ->
    Ready.

wait(?IS_FUTURE = Future, Options) ->
    case is_ready(Future) of
        true ->
            Result = get(Future),
            % Flush ready message if already sent
            flush_future_message(Future),
            Result;
        false ->
            Timeout = erlfdb_util:get(Options, timeout, infinity),
            {erlfdb_future, MsgRef, _Res} = Future,
            receive
                {MsgRef, ready} -> get(Future)
            after Timeout ->
                erlang:error({timeout, Future})
            end
    end;
wait(Ready, _) ->
    Ready.

wait_for_any(Futures) ->
    wait_for_any(Futures, []).

wait_for_any(Futures, Options) ->
    wait_for_any(Futures, Options, []).

wait_for_any(Futures, Options, ResendQ) ->
    Timeout = erlfdb_util:get(Options, timeout, infinity),
    receive
        {MsgRef, ready} = Msg ->
            case lists:keyfind(MsgRef, 2, Futures) of
                ?IS_FUTURE = Future ->
                    lists:foreach(
                        fun(M) ->
                            self() ! M
                        end,
                        ResendQ
                    ),
                    Future;
                _ ->
                    wait_for_any(Futures, Options, [Msg | ResendQ])
            end
    after Timeout ->
        lists:foreach(
            fun(M) ->
                self() ! M
            end,
            ResendQ
        ),
        erlang:error({timeout, Futures})
    end.

wait_for_all(Futures) ->
    wait_for_all(Futures, []).

wait_for_all(Futures, Options) ->
    % Same as wait for all. We might want to
    % handle timeouts here so we have a single
    % timeout for all future waiting.
    lists:map(
        fun(Future) ->
            wait(Future, Options)
        end,
        Futures
    ).

get(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        wait(get(Tx, Key))
    end);
get(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_get(Tx, Key, false);
get(?IS_SS = SS, Key) ->
    get_ss(?GET_TX(SS), Key).

get_ss(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_get(Tx, Key, true);
get_ss(?IS_SS = SS, Key) ->
    get_ss(?GET_TX(SS), Key).

get_key(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        wait(get_key(Tx, Key))
    end);
get_key(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_get_key(Tx, Key, false);
get_key(?IS_SS = SS, Key) ->
    get_key_ss(?GET_TX(SS), Key).

get_key_ss(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_get_key(Tx, Key, true).

get_range(DbOrTx, StartKey, EndKey) ->
    get_range(DbOrTx, StartKey, EndKey, []).

get_range(?IS_DB = Db, StartKey, EndKey, Options) ->
    transactional(Db, fun(Tx) ->
        get_range(Tx, StartKey, EndKey, Options)
    end);
get_range(?IS_TX = Tx, StartKey, EndKey, Options) ->
    Fun = fun(Rows, Acc) -> [Rows | Acc] end,
    Chunks = fold_range_int(Tx, StartKey, EndKey, Fun, [], Options),
    lists:flatten(lists:reverse(Chunks));
get_range(?IS_SS = SS, StartKey, EndKey, Options) ->
    get_range(?GET_TX(SS), StartKey, EndKey, [{snapshot, true} | Options]).

get_range_startswith(DbOrTx, Prefix) ->
    get_range_startswith(DbOrTx, Prefix, []).

get_range_startswith(DbOrTx, Prefix, Options) ->
    StartKey = Prefix,
    EndKey = erlfdb_key:strinc(Prefix),
    get_range(DbOrTx, StartKey, EndKey, Options).

fold_range(DbOrTx, StartKey, EndKey, Fun, Acc) ->
    fold_range(DbOrTx, StartKey, EndKey, Fun, Acc, []).

fold_range(?IS_DB = Db, StartKey, EndKey, Fun, Acc, Options) ->
    transactional(Db, fun(Tx) ->
        fold_range(Tx, StartKey, EndKey, Fun, Acc, Options)
    end);
fold_range(?IS_TX = Tx, StartKey, EndKey, Fun, Acc, Options) ->
    fold_range_int(
        Tx,
        StartKey,
        EndKey,
        fun(Rows, InnerAcc) ->
            lists:foldl(Fun, InnerAcc, Rows)
        end,
        Acc,
        Options
    );
fold_range(?IS_SS = SS, StartKey, EndKey, Fun, Acc, Options) ->
    SSOptions = [{snapshot, true} | Options],
    fold_range(?GET_TX(SS), StartKey, EndKey, Fun, Acc, SSOptions).

fold_range_future(?IS_TX = Tx, StartKey, EndKey, Options) ->
    St = options_to_fold_st(StartKey, EndKey, Options),
    fold_range_future_int(Tx, St);
fold_range_future(?IS_SS = SS, StartKey, EndKey, Options) ->
    SSOptions = [{snapshot, true} | Options],
    fold_range_future(?GET_TX(SS), StartKey, EndKey, SSOptions).

fold_range_wait(?IS_TX = Tx, ?IS_FOLD_FUTURE = FI, Fun, Acc) ->
    fold_range_int(
        Tx,
        FI,
        fun(Rows, InnerAcc) ->
            lists:foldl(Fun, InnerAcc, Rows)
        end,
        Acc
    );
fold_range_wait(?IS_SS = SS, ?IS_FOLD_FUTURE = FI, Fun, Acc) ->
    fold_range_wait(?GET_TX(SS), FI, Fun, Acc).

set(?IS_DB = Db, Key, Value) ->
    transactional(Db, fun(Tx) ->
        set(Tx, Key, Value)
    end);
set(?IS_TX = Tx, Key, Value) ->
    erlfdb_nif:transaction_set(Tx, Key, Value);
set(?IS_SS = SS, Key, Value) ->
    set(?GET_TX(SS), Key, Value).

clear(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        clear(Tx, Key)
    end);
clear(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_clear(Tx, Key);
clear(?IS_SS = SS, Key) ->
    clear(?GET_TX(SS), Key).

clear_range(?IS_DB = Db, StartKey, EndKey) ->
    transactional(Db, fun(Tx) ->
        clear_range(Tx, StartKey, EndKey)
    end);
clear_range(?IS_TX = Tx, StartKey, EndKey) ->
    erlfdb_nif:transaction_clear_range(Tx, StartKey, EndKey);
clear_range(?IS_SS = SS, StartKey, EndKey) ->
    clear_range(?GET_TX(SS), StartKey, EndKey).

clear_range_startswith(?IS_DB = Db, Prefix) ->
    transactional(Db, fun(Tx) ->
        clear_range_startswith(Tx, Prefix)
    end);
clear_range_startswith(?IS_TX = Tx, Prefix) ->
    EndKey = erlfdb_key:strinc(Prefix),
    erlfdb_nif:transaction_clear_range(Tx, Prefix, EndKey);
clear_range_startswith(?IS_SS = SS, Prefix) ->
    clear_range_startswith(?GET_TX(SS), Prefix).

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
    erlfdb_nif:transaction_atomic_op(Tx, Key, Param, Op);
atomic_op(?IS_SS = SS, Key, Param, Op) ->
    atomic_op(?GET_TX(SS), Key, Param, Op).

watch(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        watch(Tx, Key)
    end);
watch(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_watch(Tx, Key);
watch(?IS_SS = SS, Key) ->
    watch(?GET_TX(SS), Key).

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

add_read_conflict_key(TxObj, Key) ->
    add_read_conflict_range(TxObj, Key, <<Key/binary, 16#00>>).

add_read_conflict_range(TxObj, StartKey, EndKey) ->
    add_conflict_range(TxObj, StartKey, EndKey, read).

add_write_conflict_key(TxObj, Key) ->
    add_write_conflict_range(TxObj, Key, <<Key/binary, 16#00>>).

add_write_conflict_range(TxObj, StartKey, EndKey) ->
    add_conflict_range(TxObj, StartKey, EndKey, write).

add_conflict_range(?IS_TX = Tx, StartKey, EndKey, Type) ->
    erlfdb_nif:transaction_add_conflict_range(Tx, StartKey, EndKey, Type);
add_conflict_range(?IS_SS = SS, StartKey, EndKey, Type) ->
    add_conflict_range(?GET_TX(SS), StartKey, EndKey, Type).

set_read_version(?IS_TX = Tx, Version) ->
    erlfdb_nif:transaction_set_read_version(Tx, Version);
set_read_version(?IS_SS = SS, Version) ->
    set_read_version(?GET_TX(SS), Version).

get_read_version(?IS_TX = Tx) ->
    erlfdb_nif:transaction_get_read_version(Tx);
get_read_version(?IS_SS = SS) ->
    get_read_version(?GET_TX(SS)).

get_committed_version(?IS_TX = Tx) ->
    erlfdb_nif:transaction_get_committed_version(Tx);
get_committed_version(?IS_SS = SS) ->
    get_committed_version(?GET_TX(SS)).

get_versionstamp(?IS_TX = Tx) ->
    erlfdb_nif:transaction_get_versionstamp(Tx);
get_versionstamp(?IS_SS = SS) ->
    get_versionstamp(?GET_TX(SS)).

get_approximate_size(?IS_TX = Tx) ->
    erlfdb_nif:transaction_get_approximate_size(Tx);
get_approximate_size(?IS_SS = SS) ->
    get_approximate_size(?GET_TX(SS)).

get_next_tx_id(?IS_TX = Tx) ->
    erlfdb_nif:transaction_get_next_tx_id(Tx);
get_next_tx_id(?IS_SS = SS) ->
    get_next_tx_id(?GET_TX(SS)).

is_read_only(?IS_TX = Tx) ->
    erlfdb_nif:transaction_is_read_only(Tx);
is_read_only(?IS_SS = SS) ->
    is_read_only(?GET_TX(SS)).

has_watches(?IS_TX = Tx) ->
    erlfdb_nif:transaction_has_watches(Tx);
has_watches(?IS_SS = SS) ->
    has_watches(?GET_TX(SS)).

get_writes_allowed(?IS_TX = Tx) ->
    erlfdb_nif:transaction_get_writes_allowed(Tx);
get_writes_allowed(?IS_SS = SS) ->
    get_writes_allowed(?GET_TX(SS)).

get_addresses_for_key(?IS_DB = Db, Key) ->
    transactional(Db, fun(Tx) ->
        wait(get_addresses_for_key(Tx, Key))
    end);
get_addresses_for_key(?IS_TX = Tx, Key) ->
    erlfdb_nif:transaction_get_addresses_for_key(Tx, Key);
get_addresses_for_key(?IS_SS = SS, Key) ->
    get_addresses_for_key(?GET_TX(SS), Key).

get_conflicting_keys(?IS_TX = Tx) ->
    StartKey = <<16#FF, 16#FF, "/transaction/conflicting_keys/">>,
    EndKey = <<16#FF, 16#FF, "/transaction/conflicting_keys/", 16#FF>>,
    get_range(Tx, StartKey, EndKey).

on_error(?IS_TX = Tx, {erlfdb_error, ErrorCode}) ->
    on_error(Tx, ErrorCode);
on_error(?IS_TX = Tx, ErrorCode) ->
    erlfdb_nif:transaction_on_error(Tx, ErrorCode);
on_error(?IS_SS = SS, Error) ->
    on_error(?GET_TX(SS), Error).

error_predicate(Predicate, {erlfdb_error, ErrorCode}) ->
    error_predicate(Predicate, ErrorCode);
error_predicate(Predicate, ErrorCode) ->
    erlfdb_nif:error_predicate(Predicate, ErrorCode).

get_last_error() ->
    erlang:get(?ERLFDB_ERROR).

get_error_string(ErrorCode) when is_integer(ErrorCode) ->
    erlfdb_nif:get_error(ErrorCode).

clear_erlfdb_error() ->
    put(?ERLFDB_ERROR, undefined).

do_transaction(?IS_TX = Tx, UserFun) ->
    try
        Ret = UserFun(Tx),
        case is_read_only(Tx) andalso not has_watches(Tx) of
            true -> ok;
            false -> wait(commit(Tx), [{timeout, infinity}])
        end,
        Ret
    catch
        error:{erlfdb_error, Code} ->
            put(?ERLFDB_ERROR, Code),
            wait(on_error(Tx, Code), [{timeout, infinity}]),
            do_transaction(Tx, UserFun)
    end.

fold_range_int(?IS_TX = Tx, StartKey, EndKey, Fun, Acc, Options) ->
    St = options_to_fold_st(StartKey, EndKey, Options),
    fold_range_int(Tx, St, Fun, Acc).

fold_range_int(Tx, #fold_st{} = St, Fun, Acc) ->
    RangeFuture = fold_range_future_int(Tx, St),
    fold_range_int(Tx, RangeFuture, Fun, Acc);
fold_range_int(Tx, ?IS_FOLD_FUTURE = FI, Fun, Acc) ->
    {fold_info, St, Future} = FI,
    #fold_st{
        start_key = StartKey,
        end_key = EndKey,
        limit = Limit,
        iteration = Iteration,
        reverse = Reverse
    } = St,

    {RawRows, Count, HasMore} = wait(Future),

    Count = length(RawRows),

    % If our limit is within the current set of
    % rows we need to truncate the list
    Rows =
        if
            Limit == 0 orelse Limit > Count -> RawRows;
            true -> lists:sublist(RawRows, Limit)
        end,

    % Invoke our callback to update the accumulator
    NewAcc =
        if
            Rows == [] -> Acc;
            true -> Fun(Rows, Acc)
        end,

    % Determine if we have more rows to iterate
    Recurse = (Rows /= []) and (Limit == 0 orelse Limit > Count) and HasMore,

    if
        not Recurse ->
            NewAcc;
        true ->
            LastKey = element(1, lists:last(Rows)),
            {NewStartKey, NewEndKey} =
                case Reverse /= 0 of
                    true ->
                        {StartKey, erlfdb_key:first_greater_or_equal(LastKey)};
                    false ->
                        {erlfdb_key:first_greater_than(LastKey), EndKey}
                end,
            NewSt = St#fold_st{
                start_key = NewStartKey,
                end_key = NewEndKey,
                limit =
                    if
                        Limit == 0 -> 0;
                        true -> Limit - Count
                    end,
                iteration = Iteration + 1
            },
            fold_range_int(Tx, NewSt, Fun, NewAcc)
    end.

fold_range_future_int(?IS_TX = Tx, #fold_st{} = St) ->
    #fold_st{
        start_key = StartKey,
        end_key = EndKey,
        limit = Limit,
        target_bytes = TargetBytes,
        streaming_mode = StreamingMode,
        iteration = Iteration,
        snapshot = Snapshot,
        reverse = Reverse
    } = St,

    Future = erlfdb_nif:transaction_get_range(
        Tx,
        StartKey,
        EndKey,
        Limit,
        TargetBytes,
        StreamingMode,
        Iteration,
        Snapshot,
        Reverse
    ),

    {fold_info, St, Future}.

options_to_fold_st(StartKey, EndKey, Options) ->
    Reverse =
        case erlfdb_util:get(Options, reverse, false) of
            true -> 1;
            false -> 0;
            I when is_integer(I) -> I
        end,
    #fold_st{
        start_key = erlfdb_key:to_selector(StartKey),
        end_key = erlfdb_key:to_selector(EndKey),
        limit = erlfdb_util:get(Options, limit, 0),
        target_bytes = erlfdb_util:get(Options, target_bytes, 0),
        streaming_mode = erlfdb_util:get(Options, streaming_mode, want_all),
        iteration = erlfdb_util:get(Options, iteration, 1),
        snapshot = erlfdb_util:get(Options, snapshot, false),
        reverse = Reverse
    }.

flush_future_message(?IS_FUTURE = Future) ->
    erlfdb_nif:future_silence(Future),
    {erlfdb_future, MsgRef, _Res} = Future,
    receive
        {MsgRef, ready} -> ok
    after 0 -> ok
    end.
