#!/usr/bin/env escript

-mode(compile).


-define(DIRECTORY_CREATE_OPS, [
    <<"DIRECTORY_CREATE_SUBSPACE">>,
    <<"DIRECTORY_CREATE_LAYER">>,
    <<"DIRECTORY_CREATE_OR_OPEN">>,
    <<"DIRECTORY_CREATE">>,
    <<"DIRECTORY_OPEN">>,
    <<"DIRECTORY_MOVE">>,
    <<"DIRECTORY_MOVE_TO">>,
    <<"DIRECTORY_OPEN_SUBSPACE">>
]).


-record(st, {
    db,
    tx_mgr,
    tx_name,
    instructions,
    op_tuple,
    stack,
    index,
    is_db,
    is_snapshot,
    last_version,
    pids,

    % Directory Layer tests
    is_directory_op,
    dir_list,
    dir_index,
    dir_error_index
}).


init_rand() ->
    case os:getenv("RANDOM_SEED") of
        false ->
            ok;
        Seed ->
            rand:seed(exsplus, {list_to_integer(Seed), 0, 0})
    end.


stack_create() ->
    Pid = spawn_link(fun() -> stack_loop([]) end),
    spawn(fun() ->
        erlang:monitor(process, Pid),
        receive
            {'DOWN', _, _, Pid, Reason} ->
                io:format(standard_error, "STACK DIED: ~p~n", [Reason])
        end
    end),
    Pid.


stack_loop(St) ->
    receive
        Msg ->
            NewSt = stack_handle(Msg, St),
            stack_loop(NewSt)
    end.


stack_handle({From, log}, St) ->
    lists:foldl(fun(I, Pos) ->
        io:format("~p: ~p~n", [Pos, I]),
        Pos + 1
    end, 1, St),
    From ! {self(), ok},
    St;

stack_handle({From, clear}, _St) ->
    From ! {self(), ok},
    [];

stack_handle({From, length}, St) ->
    From ! {self(), length(St)},
    St;

stack_handle({From, get, Pos}, St) ->
    From ! {self(), lists:nth(Pos + 1, St)},
    St;

stack_handle({From, set, Pos, Value}, St) ->
    Head = lists:sublist(St, Pos),
    Tail = lists:nthtail(Pos + 1, St),
    NewSt = Head ++ [Value] ++ Tail,
    From ! {self(), ok},
    NewSt;

stack_handle({From, push, Value}, St) ->
    NewSt = [Value | St],
    From ! {self(), ok},
    NewSt;

stack_handle({From, pop, Count}, St) ->
    Items = lists:sublist(St, Count),
    NewSt = lists:nthtail(Count, St),
    From ! {self(), Items},
    NewSt.


%% stack_log(#st{stack = Pid}) ->
%%     stack_log(Pid);
%%
%% stack_log(Pid) ->
%%     Pid ! {self(), log},
%%     receive {Pid, ok} -> ok end.


stack_clear(#st{stack = Pid}) ->
    stack_clear(Pid);

stack_clear(Pid) ->
    Pid ! {self(), clear},
    receive {Pid, ok} -> ok end.


stack_size(#st{stack = Pid}) ->
    stack_size(Pid);

stack_size(Pid) ->
    Pid ! {self(), length},
    receive {Pid, Length} -> Length end.


stack_get(#st{stack = Pid}, Pos) ->
    stack_get(Pid, Pos);

stack_get(Pid, Pos) ->
    Pid ! {self(), get, Pos},
    receive {Pid, Value} -> Value end.


stack_set(#st{stack = Pid}, Pos, Value) ->
    stack_set(Pid, Pos, Value);

stack_set(Pid, Pos, Value) ->
    Pid ! {self(), set, Pos, Value},
    receive {Pid, ok} -> ok end.


stack_push(#st{stack = Pid, index = Idx}, Value) ->
    stack_push(Pid, {Idx, Value});

stack_push(_Pid, {_Idx, ok}) ->
    erlang:error(broken_here);
stack_push(Pid, Value) ->
    Pid ! {self(), push, Value},
    receive {Pid, ok} -> ok end.


stack_pop_with_idx(#st{stack = Pid}, Count) ->
    stack_pop_with_idx(Pid, Count);

stack_pop_with_idx(Pid, Count) ->
    Pid ! {self(), pop, Count},
    Items = receive {Pid, Items0} -> Items0 end,
    lists:map(fun({Idx, Item}) ->
        case Item of
            {erlfdb_future, _, _} = Future ->
                try
                    case erlfdb:wait(Future) of
                        ok -> {Idx, <<"RESULT_NOT_PRESENT">>};
                        not_found -> {Idx, <<"RESULT_NOT_PRESENT">>};
                        Else -> {Idx, Else}
                    end
                catch error:{erlfdb_error, Code} ->
                    CodeBin = integer_to_binary(Code),
                    ErrBin = erlfdb_tuple:pack({<<"ERROR">>, CodeBin}),
                    {Idx, ErrBin}
                end;
            _ ->
                {Idx, Item}
        end
    end, Items).


stack_pop(Obj, Count) ->
    Items = stack_pop_with_idx(Obj, Count),
    [Val || {_Idx, Val} <- Items].


stack_pop_with_idx(Obj) ->
    [Item] = stack_pop_with_idx(Obj, 1),
    Item.


stack_pop(Obj) ->
    {_Idx, Val} = stack_pop_with_idx(Obj),
    Val.


stack_push_range(St, {erlfdb_future, _, _} = Future) ->
    stack_push_range(St, erlfdb:wait(Future), <<>>);

stack_push_range(St, Results) ->
    stack_push_range(St, Results, <<>>).


stack_push_range(St, {erlfdb_future, _, _} = Future, PrefixFilter) ->
    stack_push_range(St, erlfdb:wait(Future), PrefixFilter);

stack_push_range(#st{stack = Pid, index = Idx}, Results, PrefixFilter) ->
    PFLen = size(PrefixFilter),
    List = lists:foldr(fun
        ({<<PF:PFLen/binary, _/binary>> = K, V}, Acc) when PF == PrefixFilter ->
            [K, V | Acc];
        (_, Acc) ->
            Acc
    end, [], Results),
    Value = erlfdb_tuple:pack(list_to_tuple(List)),
    stack_push(Pid, {Idx, Value}).


stack_pop_tuples(St) ->
    {Tuple} = stack_pop_tuples(St, 1),
    Tuple.


stack_pop_tuples(St, Count) ->
    TupleList = lists:map(fun(_) ->
        TupleSize = stack_pop(St),
        TupleElems = stack_pop(St, TupleSize),
        list_to_tuple(TupleElems)
    end, lists:seq(1, Count)),
    list_to_tuple(TupleList).


get_transaction(TxName) ->
    get({'$erlfdb_tx', TxName}).


new_transaction(Db, TxName) ->
    Tx = erlfdb:create_transaction(Db),
    put({'$erlfdb_tx', TxName}, Tx).


switch_transaction(Db, TxName) ->
    case get_transaction(TxName) of
        undefined ->
            new_transaction(Db, TxName);
        _ ->
            ok
    end.


has_prefix(Subject, Prefix) ->
    PrefLen = size(Prefix),
    case Subject of
        <<Prefix:PrefLen/binary, _/binary>> -> true;
        _ -> false
    end.


has_suffix(Subject, Suffix) ->
    SubjSize = size(Subject),
    SuffSize = size(Suffix),
    Offset = SubjSize - SuffSize,
    case Subject of
        <<_:Offset/binary, Suffix/binary>> -> true;
        _ -> false
    end.


log_stack(_Db, _Prefix, []) ->
    ok;

log_stack(Db, Prefix, Items) when length(Items) =< 100 ->
    erlfdb:transactional(Db, fun(Tx) ->
        lists:foreach(fun({StackPos, {Idx, Item}}) ->
            KeyTail = erlfdb_tuple:pack({StackPos, Idx}),
            Key = <<Prefix/binary, KeyTail/binary>>,
            RawVal = erlfdb_tuple:pack({Item}),
            Val = case size(RawVal) > 40000 of
                true -> binary:part(RawVal, {0, 40000});
                false -> RawVal
            end,
            erlfdb:set(Tx, Key, Val)
        end, Items)
    end);

log_stack(Db, Prefix, Items) ->
    {Head, Tail} = lists:split(100, Items),
    log_stack(Db, Prefix, Head),
    log_stack(Db, Prefix, Tail).


wait_for_empty(Db, Prefix) ->
    erlfdb:transactional(Db, fun(Tx) ->
        Future = erlfdb:get_range_startswith(Tx, Prefix, [{limit, 1}]),
        case erlfdb:wait(Future) of
            [_|_] -> erlang:error({erlfdb_error, 1020});
            [] -> ok
        end
    end).


append_dir(St, Dir) ->
    case Dir of
        not_found -> erlang:error(broken);
        _ -> ok
    end,
    #st{
        dir_list = DirList
    } = St,
    St#st{
        dir_list = DirList ++ [Dir]
    }.


init_run_loop(Db, Prefix) ->
    init_rand(),
    {StartKey, EndKey} = erlfdb_tuple:range({Prefix}),
    St = #st{
        db = Db,
        tx_name = Prefix,
        instructions = erlfdb:get_range(Db, StartKey, EndKey),
        op_tuple = undefined,
        stack = stack_create(),
        index = 0,
        is_db = undefined,
        is_snapshot = undefined,
        last_version = 0,
        pids = [],

        dir_list = [erlfdb_directory:root()],
        dir_index = 0,
        dir_error_index = 0
    },
    %% lists:foreach(fun({K, V}) ->
    %%     io:format("'~s'~n'~s'~n", [erlfdb_util:repr(K), erlfdb_util:repr(V)])
    %% end, St#st.instructions),
    run_loop(St).


run_loop(#st{instructions = []} = St) ->
    lists:foreach(fun({Pid, Ref}) ->
        receive {'DOWN', Ref, _, Pid, _} -> ok end
    end, St#st.pids);

run_loop(#st{} = St) ->
    #st{
        db = Db,
        tx_name = TxName,
        instructions = Instructions,
        index = Index
    } = St,

    [{_InstrKey, InstrVal} | RestStuctions] = Instructions,

    OpTuple = erlfdb_tuple:unpack(InstrVal),
    {utf8, Op} = element(1, OpTuple),

    %% if Op == <<"PUSH">> orelse Op == <<"SWAP">> -> ok; true ->
    %%     io:format("~b. Instruction is ~s~n", [Index, Op])
    %% end,
    %% stack_log(St),

    IsDb = has_suffix(Op, <<"_DATABASE">>),
    IsSS = has_suffix(Op, <<"_SNAPSHOT">>),
    IsDir = has_prefix(Op, <<"DIRECTORY_">>),

    OpName = if not (IsDb or IsSS) -> Op; true ->
        binary:part(Op, {0, size(Op) - 9}) % strip off _DATABASE/_SNAPSHOT
    end,

    TxObj = case {IsDb, IsSS} of
        {true, false} ->
            Db;
        {false, false} ->
            get_transaction(TxName);
        {false, true} ->
            erlfdb:snapshot(get_transaction(TxName))
    end,

    PreSt = St#st{
        op_tuple = OpTuple,
        is_db = IsDb,
        is_snapshot = IsSS,
        is_directory_op = IsDir
    },

    PostSt = try
        #st{} = execute(TxObj, PreSt, OpName)
    catch error:{erlfdb_error, Code} ->
        CodeBin = integer_to_binary(Code),
        ErrBin = erlfdb_tuple:pack({<<"ERROR">>, CodeBin}),
        stack_push(St#st.stack, {Index, ErrBin}),
        PreSt
    end,

    run_loop(PostSt#st{
        instructions = RestStuctions,
        index = Index + 1,
        op_tuple = undefined,
        is_db = undefined,
        is_snapshot = undefined,
        is_directory_op = undefined
    }).


execute(_TxObj, St, <<"PUSH">>) ->
    Value = element(2, St#st.op_tuple),
    stack_push(St, Value),
    St;

execute(_TxObj, St, <<"DUP">>) ->
    Item = stack_pop_with_idx(St),
    stack_push(St#st.stack, Item),
    stack_push(St#st.stack, Item),
    St;

execute(_TxObj, St, <<"EMPTY_STACK">>) ->
    stack_clear(St),
    St;

execute(_TxObj, St, <<"SWAP">>) ->
    Idx = stack_pop(St),
    Item1 = stack_get(St, 0),
    Item2 = stack_get(St, Idx),
    stack_set(St, Idx, Item1),
    stack_set(St, 0, Item2),
    St;

execute(_TxObj, St, <<"POP">>) ->
    stack_pop(St),
    St;

execute(_TxObj, St, <<"SUB">>) ->
    [A, B] = stack_pop(St, 2),
    stack_push(St, A - B),
    St;

execute(_TxObj, St, <<"CONCAT">>) ->
    [A, B] = stack_pop(St, 2),
    NewValue = case {A, B} of
        _ when is_integer(A), is_integer(B) ->
            A + B;
        _ when is_binary(A), is_binary(B) ->
            <<A/binary, B/binary>>
    end,
    stack_push(St, NewValue),
    St;

execute(_TxObj, St, <<"WAIT_FUTURE">>) ->
    Value = stack_pop_with_idx(St),
    stack_push(St#st.stack, Value),
    St;

execute(_TxObj, St, <<"NEW_TRANSACTION">>) ->
    new_transaction(St#st.db, St#st.tx_name),
    St;

execute(_TxObj, St, <<"USE_TRANSACTION">>) ->
    TxName = stack_pop(St),
    switch_transaction(St#st.db, TxName),
    St#st{
        tx_name = TxName
    };

execute(TxObj, St, <<"ON_ERROR">>) ->
    Error = stack_pop(St),
    Future = erlfdb:on_error(TxObj, Error),
    stack_push(St, Future),
    St;

execute(TxObj, St, <<"GET">>) ->
    Key = stack_pop(St),
    Value = case erlfdb:get(TxObj, Key) of
        {erlfdb_future, _, _} = Future ->
            erlfdb:wait(Future);
        Else ->
            Else
    end,
    case Value of
        not_found ->
            stack_push(St, <<"RESULT_NOT_PRESENT">>);
        _ ->
            stack_push(St, Value)
    end,
    St;

execute(TxObj, St, <<"GET_ESTIMATED_RANGE_SIZE">>) ->
    [StartKey, EndKey] = stack_pop(St, 2),
    Value = case erlfdb:get_estimated_range_size(TxObj, StartKey, EndKey) of
        {erlfdb_future, _, _} = Future ->
            erlfdb:wait(Future);
        Else ->
            Else
    end,
    case Value of
        Size when is_integer(Size) ->
            stack_push(St, <<"GOT_ESTIMATED_RANGE_SIZE">>);
        BadResult ->
            stack_push(St, BadResult)
    end,
    St;

execute(TxObj, St, <<"GET_KEY">>) ->
    [Key, OrEqual, Offset, Prefix] = stack_pop(St, 4),
    Selector = {Key, OrEqual, Offset},
    Value = case erlfdb:get_key(TxObj, Selector) of
        {erlfdb_future, _, _} = Future ->
            erlfdb:wait(Future);
        Else ->
            Else
    end,
    PrefixSize = size(Prefix),
    case Value of
        <<Prefix:PrefixSize/binary, _/binary>> ->
            stack_push(St, Value);
        _ when Value < Prefix ->
            stack_push(St, Prefix);
        _ ->
            stack_push(St, erlfdb_key:strinc(Prefix))
    end,
    St;

execute(TxObj, St, <<"GET_RANGE">>) ->
    [Start, End, Limit, Reverse, Mode] = stack_pop(St, 5),
    Options = [{limit, Limit}, {reverse, Reverse}, {streaming_mode, Mode}],
    Result = erlfdb:get_range(TxObj, Start, End, Options),
    stack_push_range(St, Result),
    St;

execute(TxObj, St, <<"GET_RANGE_STARTS_WITH">>) ->
    [Prefix, Limit, Reverse, Mode] = stack_pop(St, 4),
    Options = [{limit, Limit}, {reverse, Reverse}, {streaming_mode, Mode}],
    Resp = erlfdb:get_range_startswith(TxObj, Prefix, Options),
    stack_push_range(St, Resp),
    St;

execute(TxObj, St, <<"GET_RANGE_SELECTOR">>) ->
    [
        StartKey,
        StartOrEqual,
        StartOffset,
        EndKey,
        EndOrEqual,
        EndOffset,
        Limit,
        Reverse,
        Mode,
        Prefix
    ] = stack_pop(St, 10),
    Start = {StartKey, StartOrEqual, StartOffset},
    End = {EndKey, EndOrEqual, EndOffset},
    Options = [{limit, Limit}, {reverse, Reverse}, {streaming_mode, Mode}],
    Resp = erlfdb:get_range(TxObj, Start, End, Options),
    stack_push_range(St, Resp, Prefix),
    St;

execute(TxObj, St, <<"GET_READ_VERSION">>) ->
    LastVersion = erlfdb:wait(erlfdb:get_read_version(TxObj)),
    stack_push(St, <<"GOT_READ_VERSION">>),
    St#st{last_version = LastVersion};

execute(TxObj, St, <<"SET">>) ->
    [Key, Value] = stack_pop(St, 2),
    erlfdb:set(TxObj, Key, Value),
    if not St#st.is_db -> ok; true ->
        stack_push(St, <<"RESULT_NOT_PRESENT">>)
    end,
    St;

execute(_TxObj, St, <<"LOG_STACK">>) ->
    Prefix = stack_pop(St),
    StackIdx = stack_size(St) - 1,
    AllItems = stack_pop_with_idx(St, StackIdx + 1),
    InitAcc = {StackIdx, []},
    {_, RevItems} = lists:foldl(fun({Idx, Item}, {Pos, Acc}) ->
        {Pos - 1, [{Pos, {Idx, Item}} | Acc]}
    end, InitAcc, AllItems),
    log_stack(St#st.db, Prefix, RevItems),
    St;

execute(TxObj, St, <<"ATOMIC_OP">>) ->
    [{utf8, OpTypeName}, Key, Val] = stack_pop(St, 3),
    OpType = list_to_atom(string:lowercase(binary_to_list(OpTypeName))),
    ok = erlfdb:atomic_op(TxObj, Key, Val, OpType),
    if not St#st.is_db -> ok; true ->
        stack_push(St, <<"RESULT_NOT_PRESENT">>)
    end,
    St;

execute(TxObj, St, <<"SET_READ_VERSION">>) ->
    erlfdb:set_read_version(TxObj, St#st.last_version),
    St;

execute(TxObj, St, <<"CLEAR">>) ->
    Key = stack_pop(St),
    erlfdb:clear(TxObj, Key),
    if not St#st.is_db -> ok; true ->
        stack_push(St, <<"RESULT_NOT_PRESENT">>)
    end,
    St;

execute(TxObj, St, <<"CLEAR_RANGE">>) ->
    [Start, End] = stack_pop(St, 2),
    erlfdb:clear_range(TxObj, Start, End),
    if not St#st.is_db -> ok; true ->
        stack_push(St, <<"RESULT_NOT_PRESENT">>)
    end,
    St;

execute(TxObj, St, <<"CLEAR_RANGE_STARTS_WITH">>) ->
    Prefix = stack_pop(St),
    erlfdb:clear_range_startswith(TxObj, Prefix),
    if not St#st.is_db -> ok; true ->
        stack_push(St, <<"RESULT_NOT_PRESENT">>)
    end,
    St;

execute(TxObj, St, <<"READ_CONFLICT_RANGE">>) ->
    [Start, End] = stack_pop(St, 2),
    erlfdb:add_read_conflict_range(TxObj, Start, End),
    stack_push(St, <<"SET_CONFLICT_RANGE">>),
    St;

execute(TxObj, St, <<"WRITE_CONFLICT_RANGE">>) ->
    [Start, End] = stack_pop(St, 2),
    erlfdb:add_write_conflict_range(TxObj, Start, End),
    stack_push(St, <<"SET_CONFLICT_RANGE">>),
    St;

execute(TxObj, St, <<"READ_CONFLICT_KEY">>) ->
    Key = stack_pop(St),
    erlfdb:add_read_conflict_key(TxObj, Key),
    stack_push(St, <<"SET_CONFLICT_KEY">>),
    St;

execute(TxObj, St, <<"WRITE_CONFLICT_KEY">>) ->
    Key = stack_pop(St),
    erlfdb:add_write_conflict_key(TxObj, Key),
    stack_push(St, <<"SET_CONFLICT_KEY">>),
    St;

execute(TxObj, St, <<"DISABLE_WRITE_CONFLICT">>) ->
    erlfdb:set_option(TxObj, next_write_no_write_conflict_range),
    St;

execute(TxObj, St, <<"COMMIT">>) ->
    Future = erlfdb:commit(TxObj),
    stack_push(St, Future),
    St;

execute(TxObj, St, <<"RESET">>) ->
    erlfdb:reset(TxObj),
    St;

execute(TxObj, St, <<"CANCEL">>) ->
    erlfdb:cancel(TxObj),
    St;

execute(TxObj, St, <<"GET_COMMITTED_VERSION">>) ->
    Vsn = erlfdb:get_committed_version(TxObj),
    stack_push(St, <<"GOT_COMMITTED_VERSION">>),
    St#st{last_version = Vsn};

execute(TxObj, St, <<"GET_VERSIONSTAMP">>) ->
    VS = erlfdb:get_versionstamp(TxObj),
    stack_push(St, VS),
    St;

execute(TxObj, St, <<"GET_APPROXIMATE_SIZE">>) ->
    erlfdb:wait(erlfdb:get_approximate_size(TxObj)),
    stack_push(St, <<"GOT_APPROXIMATE_SIZE">>),
    St;

execute(_TxObj, St, <<"TUPLE_PACK">>) ->
    Count = stack_pop(St),
    Elems = stack_pop(St, Count),
    Val = erlfdb_tuple:pack(list_to_tuple(Elems)),
    stack_push(St, Val),
    St;

execute(_TxObj, St, <<"TUPLE_PACK_WITH_VERSIONSTAMP">>) ->
    Prefix = stack_pop(St),
    Count = stack_pop(St),
    Items = stack_pop(St, Count),
    try
        Packed = erlfdb_tuple:pack_vs(list_to_tuple(Items), Prefix),
        stack_push(St, <<"OK">>),
        stack_push(St, Packed)
    catch
        error:{erlfdb_tuple_error, missing_incomplete_versionstamp} ->
            stack_push(St, <<"ERROR: NONE">>);
        error:{erlfdb_tuple_error, multiple_incomplete_versionstamps} ->
            stack_push(St, <<"ERROR: MULTIPLE">>)
    end,
    St;

execute(_TxObj, St, <<"TUPLE_UNPACK">>) ->
    Bin = stack_pop(St),
    Tuple = erlfdb_tuple:unpack(Bin),
    lists:foreach(fun(Item) ->
        stack_push(St, erlfdb_tuple:pack({Item}))
    end, tuple_to_list(Tuple)),
    St;

execute(_TxObj, St, <<"TUPLE_SORT">>) ->
    Count = stack_pop(St),
    Elems = stack_pop(St, Count),
    Items = [erlfdb_tuple:unpack(E) || E <- Elems],
    Sorted = lists:sort(fun(A, B) ->
        erlfdb_tuple:compare(A, B) =< 0
    end, Items),
    lists:foreach(fun(Item) ->
        stack_push(St, erlfdb_tuple:pack(Item))
    end, Sorted),
    St;

execute(_TxObj, St, <<"TUPLE_RANGE">>) ->
    Count = stack_pop(St),
    Items = stack_pop(St, Count),
    {Start, End} = erlfdb_tuple:range(list_to_tuple(Items)),
    stack_push(St, Start),
    stack_push(St, End),
    St;

execute(_TxObj, St, <<"ENCODE_FLOAT">>) ->
    Val = erlfdb_float:decode(stack_pop(St)),
    stack_push(St, Val),
    St;

execute(_TxObj, St, <<"ENCODE_DOUBLE">>) ->
    Val = erlfdb_float:decode(stack_pop(St)),
    stack_push(St, Val),
    St;

execute(_TxObj, St, <<"DECODE_FLOAT">>) ->
    Val = erlfdb_float:encode(stack_pop(St)),
    stack_push(St, Val),
    St;

execute(_TxObj, St, <<"DECODE_DOUBLE">>) ->
    Val = erlfdb_float:encode(stack_pop(St)),
    stack_push(St, Val),
    St;

execute(_TxObj, St, <<"START_THREAD">>) ->
    #st{
        db = Db,
        pids = Pids
    } = St,
    Prefix = stack_pop(St),
    Pid = spawn_monitor(fun() -> init_run_loop(Db, Prefix) end),
    St#st{pids = [Pid | Pids]};

execute(_TxObj, St, <<"WAIT_EMPTY">>) ->
    Prefix = stack_pop(St),
    wait_for_empty(St#st.db, Prefix),
    stack_push(St, <<"WAITED_FOR_EMPTY">>),
    St;

execute(_TxObj, St, <<"UNIT_TESTS">>) ->
    % TODO
    St;

execute(TxObj, #st{is_directory_op = true} = St, Op) ->
    #st{
        dir_list = DirList,
        dir_index = DirIdx
    } = St,
    Dir = lists:nth(DirIdx + 1, DirList),
    try
        execute_dir(TxObj, St, Dir, Op)
    catch _T:_R ->
        NewSt = case lists:member(Op, ?DIRECTORY_CREATE_OPS) of
            true -> append_dir(St, null);
            false -> St
        end,
        stack_push(St, <<"DIRECTORY_ERROR">>),
        NewSt
    end;

execute(_TxObj, _St, UnknownOp) ->
    erlang:error({unknown_op, UnknownOp}).


execute_dir(_TxObj, St, _Dir, <<"DIRECTORY_CREATE_SUBSPACE">>) ->
    Path = stack_pop_tuples(St),
    RawPrefix = stack_pop(St),
    Subspace = erlfdb_subspace:create(Path, RawPrefix),
    append_dir(St, Subspace);

execute_dir(_TxObj, St, _Dir, <<"DIRECTORY_CREATE_LAYER">>) ->
    #st{
        dir_list = DirList
    } = St,
    [Index1, Index2, AllowManual] = stack_pop(St, 3),
    NodeSS = lists:nth(Index1 + 1, DirList),
    ContentSS = lists:nth(Index2 + 1, DirList),
    case (NodeSS == null orelse ContentSS == null) of
        true ->
            append_dir(St, null);
        false ->
            Opts = [
                {node_prefix, erlfdb_subspace:key(NodeSS)},
                {content_prefix, erlfdb_subspace:key(ContentSS)},
                {allow_manual_names, AllowManual == 1}
            ],
            append_dir(St, erlfdb_directory:root(Opts))
    end;

execute_dir(_TxObj, St, _Dir, <<"DIRECTORY_CHANGE">>) ->
    #st{
        dir_list = DirList,
        dir_error_index = ErrIdx
    } = St,
    DirIdx1 = stack_pop(St),
    DirIdx2 = case lists:nth(DirIdx1 + 1, DirList) of
        null -> ErrIdx;
        _ -> DirIdx1
    end,
    St#st{
        dir_index = DirIdx2
    };

execute_dir(_TxObj, St, _Dir, <<"DIRECTORY_SET_ERROR_INDEX">>) ->
    St#st{
        dir_error_index = stack_pop(St)
    };

execute_dir(TxObj, St, Dir, <<"DIRECTORY_CREATE_OR_OPEN">>) ->
    Path = stack_pop_tuples(St),
    Layer = stack_pop(St),
    NewDir = erlfdb_directory:create_or_open(TxObj, Dir, Path, Layer),
    append_dir(St, NewDir);

execute_dir(TxObj, St, Dir, <<"DIRECTORY_CREATE">>) ->
    Path = stack_pop_tuples(St),
    [Layer, Prefix] = stack_pop(St, 2),
    Opts = [{layer, Layer}] ++ case Prefix of
        null -> [];
        _ -> [{node_name, Prefix}]
    end,
    NewDir = erlfdb_directory:create(TxObj, Dir, Path, Opts),
    append_dir(St, NewDir);

execute_dir(TxObj, St, Dir, <<"DIRECTORY_OPEN">>) ->
    Path = stack_pop_tuples(St),
    Layer = stack_pop(St),
    Opts = [{layer, Layer}],
    NewDir = erlfdb_directory:open(TxObj, Dir, Path, Opts),
    append_dir(St, NewDir);

execute_dir(TxObj, St, Dir, <<"DIRECTORY_MOVE">>) ->
    {OldPath, NewPath} = stack_pop_tuples(St, 2),
    NewDir = erlfdb_directory:move(TxObj, Dir, OldPath, NewPath),
    append_dir(St, NewDir);

execute_dir(TxObj, St, Dir, <<"DIRECTORY_MOVE_TO">>) ->
    NewAbsPath = stack_pop_tuples(St),
    NewDir = erlfdb_directory:move_to(TxObj, Dir, NewAbsPath),
    append_dir(St, NewDir);

execute_dir(TxObj, St, Dir, <<"DIRECTORY_REMOVE">>) ->
    Count = stack_pop(St),
    case Count == 0 of
        true ->
            erlfdb_directory:remove(TxObj, Dir);
        false ->
            Path = stack_pop_tuples(St),
            erlfdb_directory:remove(TxObj, Dir, Path)
    end,
    St;

execute_dir(TxObj, St, Dir, <<"DIRECTORY_REMOVE_IF_EXISTS">>) ->
    Count = stack_pop(St),
    case Count == 0 of
        true ->
            erlfdb_directory:remove_if_exists(TxObj, Dir);
        false ->
            Path = stack_pop_tuples(St),
            erlfdb_directory:remove_if_exists(TxObj, Dir, Path)
    end,
    St;

execute_dir(TxObj, St, Dir, <<"DIRECTORY_LIST">>) ->
    Count = stack_pop(St),
    Results = case Count == 0 of
        true ->
            erlfdb_directory:list(TxObj, Dir);
        false ->
            Path = stack_pop_tuples(St),
            erlfdb_directory:list(TxObj, Dir, Path)
    end,
    Names = lists:map(fun({N, _}) -> N end, Results),
    stack_push(St, erlfdb_tuple:pack(list_to_tuple(Names))),
    St;

execute_dir(TxObj, St, Dir, <<"DIRECTORY_EXISTS">>) ->
    Count = stack_pop(St),
    Result = case Count == 0 of
        true ->
            erlfdb_directory:exists(TxObj, Dir);
        false ->
            Path = stack_pop_tuples(St),
            erlfdb_directory:exists(TxObj, Dir, Path)
    end,
    case Result of
        true -> stack_push(St, 1);
        false -> stack_push(St, 0)
    end,
    St;

execute_dir(_TxObj, St, Dir, <<"DIRECTORY_PACK_KEY">>) ->
    Tuple = stack_pop_tuples(St),
    Mod = get_dir_or_ss_mod(Dir),
    Result = Mod:pack(Dir, Tuple),
    stack_push(St, Result),
    St;

execute_dir(_TxObj, St, Dir, <<"DIRECTORY_UNPACK_KEY">>) ->
    Key = stack_pop(St),
    Mod = get_dir_or_ss_mod(Dir),
    Tuple = Mod:unpack(Dir, Key),
    lists:foreach(fun(Elem) ->
        stack_push(St, Elem)
    end, tuple_to_list(Tuple)),
    St;

execute_dir(_TxObj, St, Dir, <<"DIRECTORY_RANGE">>) ->
    Tuple = stack_pop_tuples(St),
    Mod = get_dir_or_ss_mod(Dir),
    {Start, End} = Mod:range(Dir, Tuple),
    stack_push(St, Start),
    stack_push(St, End),
    St;

execute_dir(_TxObj, St, Dir, <<"DIRECTORY_CONTAINS">>) ->
    Key = stack_pop(St),
    Mod = get_dir_or_ss_mod(Dir),
    Result = Mod:contains(Dir, Key),
    case Result of
        true -> stack_push(St, 1);
        false -> stack_push(St, 0)
    end,
    St;

execute_dir(_TxObj, St, Dir, <<"DIRECTORY_OPEN_SUBSPACE">>) ->
    Path = stack_pop_tuples(St),
    Mod = get_dir_or_ss_mod(Dir),
    Subspace = Mod:subspace(Dir, Path),
    append_dir(St, Subspace);

execute_dir(TxObj, St, Dir, <<"DIRECTORY_LOG_SUBSPACE">>) ->
    #st{
        dir_index = DirIdx
    } = St,
    Prefix = stack_pop(St),
    LogKey = erlfdb_tuple:pack({DirIdx}, Prefix),
    Mod = get_dir_or_ss_mod(Dir),
    erlfdb:set(TxObj, LogKey, Mod:key(Dir)),
    St;

execute_dir(TxObj, St, Dir, <<"DIRECTORY_LOG_DIRECTORY">>) ->
    #st{
        dir_index = DirIdx
    } = St,
    Prefix = stack_pop(St),
    LogPrefix = erlfdb_tuple:pack({DirIdx}, Prefix),

    Exists = erlfdb_directory:exists(TxObj, Dir),
    Children = case Exists of
        true ->
            ListResult = erlfdb_directory:list(TxObj, Dir),
            Names = lists:map(fun({N, _}) -> N end, ListResult),
            list_to_tuple(Names);
        false ->
            {}
    end,

    PathKey = erlfdb_tuple:pack({{utf8, <<"path">>}}, LogPrefix),
    Path = erlfdb_tuple:pack(list_to_tuple(erlfdb_directory:get_path(Dir))),
    erlfdb:set(TxObj, PathKey, Path),

    LayerKey = erlfdb_tuple:pack({{utf8, <<"layer">>}}, LogPrefix),
    Layer = erlfdb_tuple:pack({erlfdb_directory:get_layer(Dir)}),
    erlfdb:set(TxObj, LayerKey, Layer),

    ExistsKey = erlfdb_tuple:pack({{utf8, <<"exists">>}}, LogPrefix),
    ExistsVal = erlfdb_tuple:pack({if Exists -> 1; true -> 0 end}),
    erlfdb:set(TxObj, ExistsKey, ExistsVal),

    ChildrenKey = erlfdb_tuple:pack({{utf8, <<"children">>}}, LogPrefix),
    ChildrenVal = erlfdb_tuple:pack(Children),
    erlfdb:set(TxObj, ChildrenKey, ChildrenVal),

    St;

execute_dir(_TxObj, St, Dir, <<"DIRECTORY_STRIP_PREFIX">>) ->
    ToStrip = stack_pop(St),
    Mod = get_dir_or_ss_mod(Dir),
    DirKey = Mod:key(Dir),
    DKLen = size(DirKey),
    case ToStrip of
        _ when not is_binary(ToStrip) ->
            erlang:error({erlfdb_directory, prefix_not_a_binary});
        <<DirKey:DKLen/binary, Rest/binary>> ->
            stack_push(St, Rest);
        _ ->
            erlang:error({erlfdb_directory, {invalid_prefix_strip, ToStrip, DirKey}})
    end,
    St;

execute_dir(_TxObj, _St, _Dir, UnknownOp) ->
    erlang:error({unknown_directory_op, UnknownOp}).


get_dir_or_ss_mod(Subspace) when element(1, Subspace) == erlfdb_subspace ->
    erlfdb_subspace;
get_dir_or_ss_mod(#{}) ->
    erlfdb_directory.


maybe_cover_compile() ->
    case os:getenv("COVER_ENABLED") of
        Cover when Cover == false; Cover == "" ->
            ok;
        _ ->
            cover:compile_beam_directory(code:lib_dir(erlfdb, ebin))
    end.

maybe_write_coverdata(Prefix, APIVsn) ->
    case os:getenv("COVER_ENABLED") of
        Cover when Cover == false; Cover == "" ->
            ok;
        _ ->
            CoverDir = filename:join(code:lib_dir(erlfdb), "../../cover/"),
            Filename = io_lib:format("bindingtest-~s-~s.coverdata", [Prefix, APIVsn]),
            Path = filename:join(CoverDir, Filename),
            filelib:ensure_dir(Path),
            cover:export(Path)
    end.


main([Prefix, APIVsn]) ->
    main([Prefix, APIVsn, ""]);

main([Prefix, APIVsn, ClusterFileStr]) ->
    %% Prompt = io_lib:format("GDB Attach to: ~s~n", [os:getpid()]),
    %% io:get_line(Prompt),
    %% io:format("Running tests: ~s ~s ~s~n", [Prefix, APIVsn, ClusterFileStr]),

    maybe_cover_compile(),
    application:set_env(erlfdb, api_version, list_to_integer(APIVsn)),
    Db = erlfdb:open(iolist_to_binary(ClusterFileStr)),
    init_run_loop(Db, iolist_to_binary(Prefix)),
    maybe_write_coverdata(Prefix, APIVsn).
