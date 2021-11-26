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

-module(erlfdb_02_anon_fdbserver_test).

-include_lib("eunit/include/eunit.hrl").

basic_init_test() ->
    {ok, ClusterFile} = erlfdb_util:init_test_cluster([]),
    ?assert(is_binary(ClusterFile)).

basic_open_test() ->
    {ok, ClusterFile} = erlfdb_util:init_test_cluster([]),
    Db = erlfdb:open(ClusterFile),
    erlfdb:transactional(Db, fun(Tx) ->
        ?assert(true)
    end).

get_db_test() ->
    Db = erlfdb_util:get_test_db(),
    erlfdb:transactional(Db, fun(Tx) ->
        ?assert(true)
    end).

get_set_get_test() ->
    Db = erlfdb_util:get_test_db(),
    Key = gen_key(8),
    Val = crypto:strong_rand_bytes(8),
    erlfdb:transactional(Db, fun(Tx) ->
        ?assertEqual(not_found, erlfdb:wait(erlfdb:get(Tx, Key)))
    end),
    erlfdb:transactional(Db, fun(Tx) ->
        ?assertEqual(ok, erlfdb:set(Tx, Key, Val))
    end),
    erlfdb:transactional(Db, fun(Tx) ->
        ?assertEqual(Val, erlfdb:wait(erlfdb:get(Tx, Key)))
    end).

get_empty_test() ->
    Db1 = erlfdb_util:get_test_db(),
    Key = gen_key(8),
    Val = crypto:strong_rand_bytes(8),
    erlfdb:transactional(Db1, fun(Tx) ->
        ok = erlfdb:set(Tx, Key, Val)
    end),
    erlfdb:transactional(Db1, fun(Tx) ->
        ?assertEqual(Val, erlfdb:wait(erlfdb:get(Tx, Key)))
    end),

    % Check we can get an empty db
    Db2 = erlfdb_util:get_test_db([empty]),
    erlfdb:transactional(Db2, fun(Tx) ->
        ?assertEqual(not_found, erlfdb:wait(erlfdb:get(Tx, Key)))
    end),

    % And check state that the old db handle is
    % the same
    erlfdb:transactional(Db1, fun(Tx) ->
        ?assertEqual(not_found, erlfdb:wait(erlfdb:get(Tx, Key)))
    end).

gen_key(Size) when is_integer(Size), Size > 1 ->
    RandBin = crypto:strong_rand_bytes(Size - 1),
    <<0, RandBin/binary>>.
