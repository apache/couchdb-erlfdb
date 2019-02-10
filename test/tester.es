#!/usr/bin/env escript
%%! -pa /Users/davisp/github/labs-cloudant/couchdb-erlfdb/ebin

-mode(compile).


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
    directory_extension
}).


py_repr(Bin) when is_binary(Bin) ->
    [$'] ++ lists:map(fun(C) ->
        case C of
            %% 7 ->
            %%     "\\a";
            %% 8 ->
            %%     "\\b";
            9 ->
                "\\t";
            10 ->
                "\\n";
            %% 11 ->
            %%     "\\v";
            %% 12 ->
            %%     "\\f";
            13 ->
                "\\r";
            39 ->
                "\\'";
            92 ->
                "\\\\";
            _ when C >= 32, C =< 126 ->
                C;
            _ ->
                io_lib:format("\\x~2.16.0b", [C])
        end
    end, binary_to_list(Bin)) ++ [$'].


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


tx_manager_init(Db) ->
    erlang:spawn_link(fun() ->
        Tid = ets:new(tx_table, []),
        tx_manager_loop(Db, Tid)
    end).


tx_manager_loop(Db, Tid) ->
    receive
        {From, get, TxName} ->
            case ets:lookup(Tid, TxName) of
                [] -> From ! {self(), undefined};
                [{_, Tx}] -> From ! {self(), Tx}
            end;
        {From, new, TxName} ->
            Tx = erlfdb:create_transaction(Db),
            true = ets:insert(Tid, {TxName, Tx}),
            From ! {self(), Tx};
        {From, switch, TxName} ->
            case ets:lookup(Tid, TxName) of
                [] ->
                    ets:insert(Tid, {TxName, erlfdb:create_transaction(Db)});
                [_] ->
                    ok
            end,
            From ! {self(), ok}
    end,
    tx_manager_loop(Db, Tid).


get_transaction(TxPid, TxName) ->
    TxPid ! {self(), get, TxName},
    receive {TxPid, Tx} -> Tx end.


new_transaction(TxPid, TxName) ->
    TxPid ! {self(), new, TxName},
    receive {TxPid, Tx} -> Tx end.


switch_transaction(TxPid, TxName) ->
    TxPid ! {self(), switch, TxName},
    receive {TxPid, ok} -> ok end.


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
    erlfdb:transactional(Db, fun(Tr) ->
        Future = erlfdb:get_range_startswith(Tr, Prefix, [{limit, 1}]),
        case erlfdb:wait(Future) of
            [_|_] -> erlang:error({erlfdb_error, 1020});
            [] -> ok
        end
    end).


init_run_loop(Db, Prefix, TxMgr) ->
    {StartKey, EndKey} = erlfdb_tuple:range({Prefix}),
    St = #st{
        db = Db,
        tx_mgr = TxMgr,
        tx_name = Prefix,
        instructions = erlfdb:get_range(Db, StartKey, EndKey),
        op_tuple = undefined,
        stack = stack_create(),
        index = 0,
        is_db = undefined,
        is_snapshot = undefined,
        last_version = 0,
        pids = [],
        directory_extension = undefined
    },
    %% lists:foreach(fun({K, V}) ->
    %%     io:format("'~s'~n'~s'~n", [py_repr(K), py_repr(V)])
    %% end, St#st.instructions),
    run_loop(St).


run_loop(#st{instructions = []} = St) ->
    lists:foreach(fun(Pid) ->
        receive {'DOWN', _, _, Pid, _} -> ok end
    end, St#st.pids);

run_loop(#st{} = St) ->
    #st{
        db = Db,
        tx_mgr = TxMgr,
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

    OpName = if not (IsDb or IsSS) -> Op; true ->
        binary:part(Op, {0, size(Op) - 9}) % strip off _DATABASE/_SNAPSHOT
    end,

    PreSt = St#st{
        op_tuple = OpTuple,
        is_db = IsDb,
        is_snapshot = IsSS
    },

    TxObj = if IsDb -> Db; true ->
        get_transaction(TxMgr, TxName)
    end,

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
        is_snapshot = undefined
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
    exit(St#st.stack, kill),
    St#st{stack = stack_create()};

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
    new_transaction(St#st.tx_mgr, St#st.tx_name),
    St;

execute(_TxObj, St, <<"USE_TRANSACTION">>) ->
    TxName = stack_pop(St),
    switch_transaction(St#st.tx_mgr, TxName),
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
    FunName = if
        St#st.is_snapshot -> get_ss;
        true -> get
    end,
    Value = case erlfdb:FunName(TxObj, Key) of
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

execute(TxObj, St, <<"GET_KEY">>) ->
    [Key, OrEqual, Offset, Prefix] = stack_pop(St, 4),
    FunName = if
        St#st.is_snapshot -> get_key_ss;
        true -> get_key
    end,
    Selector = {Key, OrEqual, Offset},
    Value = case erlfdb:FunName(TxObj, Selector) of
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
    BaseOpts = [{limit, Limit}, {reverse, Reverse}, {streaming_mode, Mode}],
    Options = if not St#st.is_snapshot -> BaseOpts; true ->
        [{snapshot, true} | BaseOpts]
    end,
    Result = erlfdb:get_range(TxObj, Start, End, Options),
    stack_push_range(St, Result),
    St;

execute(TxObj, St, <<"GET_RANGE_STARTS_WITH">>) ->
    [Prefix, Limit, Reverse, Mode] = stack_pop(St, 4),
    BaseOpts = [{limit, Limit}, {reverse, Reverse}, {streaming_mode, Mode}],
    Options = if not St#st.is_snapshot -> BaseOpts; true ->
        [{snapshot, true} | BaseOpts]
    end,
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
    BaseOpts = [{limit, Limit}, {reverse, Reverse}, {streaming_mode, Mode}],
    Options = if not St#st.is_snapshot -> BaseOpts; true ->
        [{snapshot, true} | BaseOpts]
    end,
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
        tx_mgr = TxMgr,
        pids = Pids
    } = St,
    Prefix = stack_pop(St),
    Pid = spawn_monitor(fun() -> init_run_loop(Db, Prefix, TxMgr) end),
    St#st{pids = [Pid | Pids]};

execute(_TxObj, St, <<"WAIT_EMPTY">>) ->
    Prefix = stack_pop(St),
    wait_for_empty(St, Prefix),
    stack_push(St, <<"WAITED_FOR_EMPTY">>),
    St;

execute(_TxObj, St, <<"UNIT_TESTS">>) ->
    % TODO
    St;

execute(_TxObj, _St, UnknownOp) ->
    erlang:error({unknown_op, UnknownOp}).



main([Prefix, APIVsn]) ->
    main([Prefix, APIVsn, ""]);

main([Prefix, APIVsn, ClusterFileStr]) ->
    py_repr(<<"foo">>),
    %% Prompt = io_lib:format("GDB Attach to: ~s~n", [os:getpid()]),
    %% io:get_line(Prompt),
    %% io:format("Running tests: ~s ~s ~s~n", [Prefix, APIVsn, ClusterFileStr]),

    application:set_env(erlfdb, api_version, list_to_integer(APIVsn)),
    Db = erlfdb:open(iolist_to_binary(ClusterFileStr)),
    TxMgr = tx_manager_init(Db),
    init_run_loop(Db, iolist_to_binary(Prefix), TxMgr).
