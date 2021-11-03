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

-module(erlfdb_tuple).

-export([
    pack/1,
    pack/2,
    pack_vs/1,
    pack_vs/2,
    unpack/1,
    unpack/2,
    range/1,
    range/2,
    compare/2
]).

% Codes 16#03, 16#04, 16#23, and 16#24 are reserved
% for historical reasons.

% NULL is a single byte
-define(NULL, 16#00).

% Bytes/Strings - Variable length, ends at the
% first \x00 that's not followed by \xFF
-define(BYTES, 16#01).
-define(STRING, 16#02).

% Tuples all the way down
-define(NESTED, 16#05).

% Negative numbers: 9-255 byte numbers, then 8-1
-define(NEG_INT_9P, 16#0B).
-define(NEG_INT_8, 16#0C).
-define(NEG_INT_7, 16#0D).
-define(NEG_INT_6, 16#0E).
-define(NEG_INT_5, 16#0F).
-define(NEG_INT_4, 16#10).
-define(NEG_INT_3, 16#11).
-define(NEG_INT_2, 16#12).
-define(NEG_INT_1, 16#13).

% Zero is a single byte
-define(ZERO, 16#14).

% Positive numbers: 1-8 bytes, then all 9-255 byte numbers
-define(POS_INT_1, 16#15).
-define(POS_INT_2, 16#16).
-define(POS_INT_3, 16#17).
-define(POS_INT_4, 16#18).
-define(POS_INT_5, 16#19).
-define(POS_INT_6, 16#1A).
-define(POS_INT_7, 16#1B).
-define(POS_INT_8, 16#1C).
-define(POS_INT_9P, 16#1D).

% Floats and Doubles
-define(FLOAT, 16#20).
-define(DOUBLE, 16#21).

% Booleans are a single byte each
-define(FALSE, 16#26).
-define(TRUE, 16#27).

% UUIDs: single code byte followed by 16 bytes
-define(UUID, 16#30).

% 64 bit identifer
-define(ID64, 16#31).

% 80 bit VersionStamp: 8 byte integer, 2 byte batch
-define(VS80, 16#32).

% 96 bit VersionStamp: 8 byte integer, 2 byte
% batch, 2 byte transaction order
-define(VS96, 16#33).

% Not an actual type code but reserved
% for use when escaping \x00 bytes
-define(ESCAPE, 16#FF).

-define(UNSET_VERSIONSTAMP80, <<16#FFFFFFFFFFFFFFFFFFFF:80>>).
-define(UNSET_VERSIONSTAMP96, <<16#FFFFFFFFFFFFFFFFFFFF:80, _:16>>).

pack(Tuple) when is_tuple(Tuple) ->
    pack(Tuple, <<>>).

pack(Tuple, Prefix) ->
    Elems = tuple_to_list(Tuple),
    Encoded = [encode(E, 0) || E <- Elems],
    iolist_to_binary([Prefix | Encoded]).

pack_vs(Tuple) ->
    pack_vs(Tuple, <<>>).

pack_vs(Tuple, Prefix) ->
    Elems = tuple_to_list(Tuple),
    Encoded = [encode(E, 0) || E <- Elems],
    case find_incomplete_versionstamp(Encoded) of
        {found, Pos} ->
            VsnPos = Pos + size(Prefix),
            Parts = [Prefix, Encoded, <<VsnPos:32/unsigned-little>>],
            iolist_to_binary(Parts);
        {not_found, _} ->
            E = {erlfdb_tuple_error, missing_incomplete_versionstamp},
            erlang:error(E)
    end.

unpack(Binary) ->
    unpack(Binary, <<>>).

unpack(Binary, Prefix) ->
    PrefixLen = size(Prefix),
    case Binary of
        <<Prefix:PrefixLen/binary, Rest/binary>> ->
            case decode(Rest, 0) of
                {Elems, <<>>} ->
                    list_to_tuple(Elems);
                {_, Tail} ->
                    erlang:error({invalid_trailing_data, Tail})
            end;
        _ ->
            E = {erlfdb_tuple_error, invalid_unpack_prefix},
            erlang:error(E)
    end.

% Returns a {StartKey, EndKey} pair of binaries
% that includes all possible sub-tuples
range(Tuple) ->
    range(Tuple, <<>>).

range(Tuple, Prefix) ->
    Base = pack(Tuple, Prefix),
    {<<Base/binary, 16#00>>, <<Base/binary, 16#FF>>}.

compare(A, B) when is_tuple(A), is_tuple(B) ->
    AElems = tuple_to_list(A),
    BElems = tuple_to_list(B),
    compare_impl(AElems, BElems).

compare_impl([], []) ->
    0;
compare_impl([], [_ | _]) ->
    -1;
compare_impl([_ | _], []) ->
    1;
compare_impl([A | RestA], [B | RestB]) ->
    case compare_elems(A, B) of
        -1 -> -1;
        0 -> compare_impl(RestA, RestB);
        1 -> 1
    end.

%% erlfmt-ignore
encode(null, 0) ->
    <<?NULL>>;

encode(null, Depth) when Depth > 0 ->
    [<<?NULL, ?ESCAPE>>];

encode(false, _) ->
    <<?FALSE>>;

encode(true, _) ->
    <<?TRUE>>;

encode({utf8, Bin}, _) when is_binary(Bin) ->
    [<<?STRING>>, enc_null_terminated(Bin)];

encode({float, F} = Float, _) when is_float(F) ->
    [<<?FLOAT>>, enc_float(Float)];

encode({float, _, _F} = Float, _) ->
    [<<?FLOAT>>, enc_float(Float)];

encode({double, _, _D} = Double, _) ->
    [<<?DOUBLE>>, enc_float(Double)];

encode({uuid, Bin}, _) when is_binary(Bin), size(Bin) == 16 ->
    [<<?UUID>>, Bin];

encode({id64, Int}, _) ->
    [<<?ID64>>, <<Int:64/big>>];

encode({versionstamp, Id, Batch}, _) ->
    [<<?VS80>>, <<Id:64/big, Batch:16/big>>];

encode({versionstamp, Id, Batch, Tx}, _) ->
    [<<?VS96>>, <<Id:64/big, Batch:16/big, Tx:16/big>>];

encode(Bin, _Depth) when is_binary(Bin) ->
    [<<?BYTES>>, enc_null_terminated(Bin)];

encode(Int, _Depth) when is_integer(Int), Int < 0 ->
    Bin1 = binary:encode_unsigned(-Int),
    % Take the one's complement so that
    % they sort properly
    Bin2 = << <<(B bxor 16#FF)>> || <<B>> <= Bin1 >>,
    case size(Bin2) of
        1 -> [<<?NEG_INT_1>>, Bin2];
        2 -> [<<?NEG_INT_2>>, Bin2];
        3 -> [<<?NEG_INT_3>>, Bin2];
        4 -> [<<?NEG_INT_4>>, Bin2];
        5 -> [<<?NEG_INT_5>>, Bin2];
        6 -> [<<?NEG_INT_6>>, Bin2];
        7 -> [<<?NEG_INT_7>>, Bin2];
        8 -> [<<?NEG_INT_8>>, Bin2];
        N when N =< 255 -> [<<?NEG_INT_9P, (N bxor 16#FF)>>, Bin2]
    end;

encode(0, _) ->
    [<<?ZERO>>];

encode(Int, _Depth) when is_integer(Int), Int > 0 ->
    Bin = binary:encode_unsigned(Int),
    case size(Bin) of
        1 -> [<<?POS_INT_1>>, Bin];
        2 -> [<<?POS_INT_2>>, Bin];
        3 -> [<<?POS_INT_3>>, Bin];
        4 -> [<<?POS_INT_4>>, Bin];
        5 -> [<<?POS_INT_5>>, Bin];
        6 -> [<<?POS_INT_6>>, Bin];
        7 -> [<<?POS_INT_7>>, Bin];
        8 -> [<<?POS_INT_8>>, Bin];
        N when N =< 255 -> [<<?POS_INT_9P, N>>, Bin]
    end;

encode(Double, _) when is_float(Double) ->
    [<<?DOUBLE>>, enc_float(Double)];

encode(Tuple, Depth) when is_tuple(Tuple) ->
    Elems = tuple_to_list(Tuple),
    Encoded = [encode(E, Depth + 1) || E <- Elems],
    [<<?NESTED>>, Encoded, <<?NULL>>];

encode(BadTerm, _) ->
    erlang:error({invalid_tuple_term, BadTerm}).

enc_null_terminated(Bin) ->
    enc_null_terminated(Bin, 0).

enc_null_terminated(Bin, Offset) ->
    case Bin of
        <<Head:Offset/binary>> ->
            [Head, <<?NULL>>];
        <<Head:Offset/binary, ?NULL, Tail/binary>> ->
            [Head, <<?NULL, ?ESCAPE>> | enc_null_terminated(Tail, 0)];
        <<_Head:Offset/binary, _, _Tail/binary>> = Bin ->
            enc_null_terminated(Bin, Offset + 1)
    end.

enc_float(Float) ->
    Bin = erlfdb_float:encode(Float),
    case Bin of
        <<0:1, B:7, Rest/binary>> ->
            <<1:1, B:7, Rest/binary>>;
        <<1:1, _:7, _/binary>> ->
            <<<<(B bxor 16#FF)>> || <<B>> <= Bin>>
    end.

%% erlfmt-ignore
decode(<<>>, 0) ->
    {[], <<>>};

decode(<<?NULL, Rest/binary>>, 0) ->
    {Values, Tail} = decode(Rest, 0),
    {[null | Values], Tail};

decode(<<?NULL, ?ESCAPE, Rest/binary>>, Depth) when Depth > 0 ->
    {Values, Tail} = decode(Rest, Depth),
    {[null | Values], Tail};

decode(<<?NULL, Rest/binary>>, Depth) when Depth > 0 ->
    {[], Rest};

decode(<<?BYTES, Rest/binary>>, Depth) ->
    {Bin, NewRest} = dec_null_terminated(Rest),
    {Values, Tail} = decode(NewRest, Depth),
    {[Bin | Values], Tail};

decode(<<?STRING, Rest/binary>>, Depth) ->
    {Bin, NewRest} = dec_null_terminated(Rest),
    {Values, Tail} = decode(NewRest, Depth),
    {[{utf8, Bin} | Values], Tail};

decode(<<?NESTED, Rest/binary>>, Depth) ->
    {NestedValues, Tail1} = decode(Rest, Depth + 1),
    {RestValues, Tail2} = decode(Tail1, Depth),
    NestedTuple = list_to_tuple(NestedValues),
    {[NestedTuple | RestValues], Tail2};

decode(<<?NEG_INT_9P, InvertedSize:8, Rest/binary>>, Depth) ->
    Size = InvertedSize bxor 16#FF,
    dec_neg_int(Rest, Size, Depth);

decode(<<?NEG_INT_8, Rest/binary>>, Depth) -> dec_neg_int(Rest, 8, Depth);
decode(<<?NEG_INT_7, Rest/binary>>, Depth) -> dec_neg_int(Rest, 7, Depth);
decode(<<?NEG_INT_6, Rest/binary>>, Depth) -> dec_neg_int(Rest, 6, Depth);
decode(<<?NEG_INT_5, Rest/binary>>, Depth) -> dec_neg_int(Rest, 5, Depth);
decode(<<?NEG_INT_4, Rest/binary>>, Depth) -> dec_neg_int(Rest, 4, Depth);
decode(<<?NEG_INT_3, Rest/binary>>, Depth) -> dec_neg_int(Rest, 3, Depth);
decode(<<?NEG_INT_2, Rest/binary>>, Depth) -> dec_neg_int(Rest, 2, Depth);
decode(<<?NEG_INT_1, Rest/binary>>, Depth) -> dec_neg_int(Rest, 1, Depth);

decode(<<?ZERO, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[0 | Values], Tail};

decode(<<?POS_INT_1, Rest/binary>>, Depth) -> dec_pos_int(Rest, 1, Depth);
decode(<<?POS_INT_2, Rest/binary>>, Depth) -> dec_pos_int(Rest, 2, Depth);
decode(<<?POS_INT_3, Rest/binary>>, Depth) -> dec_pos_int(Rest, 3, Depth);
decode(<<?POS_INT_4, Rest/binary>>, Depth) -> dec_pos_int(Rest, 4, Depth);
decode(<<?POS_INT_5, Rest/binary>>, Depth) -> dec_pos_int(Rest, 5, Depth);
decode(<<?POS_INT_6, Rest/binary>>, Depth) -> dec_pos_int(Rest, 6, Depth);
decode(<<?POS_INT_7, Rest/binary>>, Depth) -> dec_pos_int(Rest, 7, Depth);
decode(<<?POS_INT_8, Rest/binary>>, Depth) -> dec_pos_int(Rest, 8, Depth);

decode(<<?POS_INT_9P, Size:8/unsigned-integer, Rest/binary>>, Depth) ->
    dec_pos_int(Rest, Size, Depth);

decode(<<?FLOAT, Raw:4/binary, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[dec_float(Raw) | Values], Tail};

decode(<<?DOUBLE, Raw:8/binary, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[dec_float(Raw) | Values], Tail};

decode(<<?FALSE, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[false | Values], Tail};

decode(<<?TRUE, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[true | Values], Tail};

decode(<<?UUID, UUID:16/binary, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[{uuid, UUID} | Values], Tail};

decode(<<?ID64, Id:64/big, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[{id64, Id} | Values], Tail};

decode(<<?VS80, Id:64/big, Batch:16/big, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[{versionstamp, Id, Batch} | Values], Tail};

decode(<<?VS96, Id:64/big, Batch:16/big, Tx:16/big, Rest/binary>>, Depth) ->
    {Values, Tail} = decode(Rest, Depth),
    {[{versionstamp, Id, Batch, Tx} | Values], Tail}.

dec_null_terminated(Bin) ->
    {Parts, Tail} = dec_null_terminated(Bin, 0),
    {iolist_to_binary(Parts), Tail}.

dec_null_terminated(Bin, Offset) ->
    case Bin of
        <<Head:Offset/binary, ?NULL, ?ESCAPE, Tail/binary>> ->
            {Parts, RestTail} = dec_null_terminated(Tail, 0),
            {[Head, <<?NULL>> | Parts], RestTail};
        <<Head:Offset/binary, ?NULL, Tail/binary>> ->
            {[Head], Tail};
        <<_:Offset/binary, _, _/binary>> ->
            dec_null_terminated(Bin, Offset + 1);
        <<_Head:Offset/binary>> ->
            erlang:error({invalid_null_termination, Bin})
    end.

dec_neg_int(Bin, Size, Depth) ->
    case Bin of
        <<Raw:Size/integer-unit:8, Rest/binary>> ->
            Val = Raw - (1 bsl (Size * 8)) + 1,
            {Values, Tail} = decode(Rest, Depth),
            {[Val | Values], Tail};
        _ ->
            erlang:error({invalid_negative_int, Size, Bin})
    end.

dec_pos_int(Bin, Size, Depth) ->
    case Bin of
        <<Val:Size/integer-unit:8, Rest/binary>> ->
            {Values, Tail} = decode(Rest, Depth),
            {[Val | Values], Tail};
        _ ->
            erlang:error({invalid_positive_int, Size, Bin})
    end.

dec_float(<<0:1, _:7, _/binary>> = Bin) ->
    erlfdb_float:decode(<<<<(B bxor 16#FF)>> || <<B>> <= Bin>>);
dec_float(<<Byte:8/integer, Rest/binary>>) ->
    erlfdb_float:decode(<<(Byte bxor 16#80):8/integer, Rest/binary>>).

find_incomplete_versionstamp(Items) ->
    find_incomplete_versionstamp(Items, 0).

find_incomplete_versionstamp([], Pos) ->
    {not_found, Pos};
find_incomplete_versionstamp([<<?VS80>>, ?UNSET_VERSIONSTAMP80 | Rest], Pos) ->
    case find_incomplete_versionstamp(Rest, Pos + 11) of
        {not_found, _} ->
            {found, Pos + 1};
        {found, _} ->
            E = {erlfdb_tuple_error, multiple_incomplete_versionstamps},
            erlang:error(E)
    end;
find_incomplete_versionstamp([<<?VS96>>, ?UNSET_VERSIONSTAMP96 | Rest], Pos) ->
    case find_incomplete_versionstamp(Rest, Pos + 13) of
        {not_found, _} ->
            {found, Pos + 1};
        {found, _} ->
            E = {erlfdb_tuple_error, multiple_incomplete_versionstamps},
            erlang:error(E)
    end;
find_incomplete_versionstamp([Item | Rest], Pos) when is_list(Item) ->
    case find_incomplete_versionstamp(Item, Pos) of
        {not_found, NewPos} ->
            find_incomplete_versionstamp(Rest, NewPos);
        {found, FoundPos} ->
            % The second versionstamp that is found will not
            % report a correct version. For now that's fine
            % as more than one version stamp is an error.
            case find_incomplete_versionstamp(Rest, Pos) of
                {not_found, _} ->
                    {found, FoundPos};
                {found, _} ->
                    E = {erlfdb_tuple_error, multiple_incomplete_versionstamps},
                    erlang:error(E)
            end
    end;
find_incomplete_versionstamp([Bin | Rest], Pos) when is_binary(Bin) ->
    find_incomplete_versionstamp(Rest, Pos + size(Bin)).

compare_elems(A, B) ->
    CodeA = code_for(A),
    CodeB = code_for(B),

    case {code_for(A), code_for(B)} of
        {CodeA, CodeB} when CodeA < CodeB ->
            -1;
        {CodeA, CodeB} when CodeA > CodeB ->
            1;
        {Code, Code} when Code == ?NULL ->
            0;
        {Code, Code} when Code == ?FLOAT; Code == ?DOUBLE ->
            compare_floats(A, B);
        {Code, Code} when Code == ?NESTED ->
            compare(A, B);
        {Code, Code} when A < B ->
            -1;
        {Code, Code} when A > B ->
            1;
        {Code, Code} when A == B ->
            0
    end.

compare_floats(F1, F2) ->
    B1 = pack({F1}),
    B2 = pack({F2}),
    if
        B1 < B2 -> -1;
        B1 > B2 -> 1;
        true -> 0
    end.

code_for(null) -> ?NULL;
code_for(<<_/binary>>) -> ?BYTES;
code_for({utf8, <<_/binary>>}) -> ?STRING;
code_for(I) when is_integer(I) -> ?ZERO;
code_for({float, F}) when is_float(F) -> ?FLOAT;
code_for({float, _, B}) when is_binary(B) -> ?FLOAT;
code_for(F) when is_float(F) -> ?DOUBLE;
code_for({double, _, B}) when is_binary(B) -> ?DOUBLE;
code_for(false) -> ?FALSE;
code_for(true) -> ?TRUE;
code_for({uuid, <<_:16/binary>>}) -> ?UUID;
code_for({id64, _Id}) -> ?ID64;
code_for({versionstamp, _Id, _Batch}) -> ?VS80;
code_for({versionstamp, _Id, _Batch, _Tx}) -> ?VS96;
code_for(T) when is_tuple(T) -> ?NESTED;
code_for(Bad) -> erlang:error({invalid_tuple_element, Bad}).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

fdb_vectors_test() ->
    % These are all from the tuple layer spec:
    %     https://github.com/apple/foundationdb/blob/master/design/tuple.md
    Cases = [
        {
            {null},
            <<0>>
        },
        {
            {<<"foo", 16#00, "bar">>},
            <<16#01, "foo", 16#00, 16#FF, "bar", 16#00>>
        },
        {
            {{utf8, unicode:characters_to_binary(["F", 16#D4, "O", 16#00, "bar"])}},
            <<16#02, "F", 16#C3, 16#94, "O", 16#00, 16#FF, "bar", 16#00>>
        },
        {
            {{<<"foo", 16#00, "bar">>, null, {}}},
            <<16#05, 16#01, "foo", 16#00, 16#FF, "bar", 16#00, 16#00, 16#FF, 16#05, 16#00, 16#00>>
        },
        {
            {-5551212},
            <<16#11, 16#AB, 16#4B, 16#93>>
        },
        {
            {{float, -42.0}},
            <<16#20, 16#3D, 16#D7, 16#FF, 16#FF>>
        }
    ],
    lists:foreach(
        fun({Test, Expect}) ->
            ?assertEqual(Expect, pack(Test)),
            ?assertEqual(Test, unpack(pack(Test)))
        end,
        Cases
    ).

bindingstester_discoveries_test() ->
    Pairs = [
        {<<33, 134, 55, 204, 184, 171, 21, 15, 128>>, {1.048903115625475e-278}}
    ],
    lists:foreach(
        fun({Packed, Unpacked}) ->
            ?assertEqual(Packed, pack(Unpacked)),
            ?assertEqual(Unpacked, unpack(Packed))
        end,
        Pairs
    ),

    PackUnpackCases = [
        {-87469399449579948399912777925908893746277401541675257432930}
    ],
    lists:foreach(
        fun(Test) ->
            ?assertEqual(Test, unpack(pack(Test)))
        end,
        PackUnpackCases
    ),

    UnpackPackCases = [
        <<33, 134, 55, 204, 184, 171, 21, 15, 128>>
    ],
    lists:foreach(
        fun(Test) ->
            ?assertEqual(Test, pack(unpack(Test)))
        end,
        UnpackPackCases
    ).

-endif.
