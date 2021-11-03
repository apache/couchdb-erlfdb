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

-module(erlfdb_04_snapshot_test).

-include_lib("eunit/include/eunit.hrl").

snapshot_from_tx_test() ->
    Db = erlfdb_util:get_test_db(),
    Key = gen(10),
    Val = gen(10),
    erlfdb:set(Db, Key, Val),
    erlfdb:transactional(Db, fun(Tx) ->
        ?assertEqual(Val, erlfdb:wait(erlfdb:get_ss(Tx, Key))),
        Ss = erlfdb:snapshot(Tx),
        ?assertEqual(Val, erlfdb:wait(erlfdb:get(Ss, Key)))
    end).

snapshot_from_a_snapshot_test() ->
    Db = erlfdb_util:get_test_db(),
    Key = gen(10),
    Val = gen(10),
    erlfdb:set(Db, Key, Val),
    erlfdb:transactional(Db, fun(Tx) ->
        Ss = erlfdb:snapshot(Tx),
        ?assertEqual(Val, erlfdb:wait(erlfdb:get_ss(Ss, Key))),
        Ss = erlfdb:snapshot(Ss)
    end).

gen(Size) when is_integer(Size), Size > 1 ->
    RandBin = crypto:strong_rand_bytes(Size - 1),
    <<0, RandBin/binary>>.
