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

-module(erlfdb_03_transaction_options_test).

-include_lib("eunit/include/eunit.hrl").

get_approximate_tx_size_test() ->
    Db1 = erlfdb_util:get_test_db(),
    erlfdb:transactional(Db1, fun(Tx) ->
        ok = erlfdb:set(Tx, gen(10), gen(5000)),
        TxSize1 = erlfdb:wait(erlfdb:get_approximate_size(Tx)),
        ?assert(TxSize1 > 5000 andalso TxSize1 < 6000),
        ok = erlfdb:set(Tx, gen(10), gen(5000)),
        TxSize2 = erlfdb:wait(erlfdb:get_approximate_size(Tx)),
        ?assert(TxSize2 > 10000)
    end).

size_limit_test() ->
    Db1 = erlfdb_util:get_test_db(),
    ?assertError(
        {erlfdb_error, 2101},
        erlfdb:transactional(Db1, fun(Tx) ->
            erlfdb:set_option(Tx, size_limit, 10000),
            erlfdb:set(Tx, gen(10), gen(11000))
        end)
    ).

writes_allowed_test() ->
    Db1 = erlfdb_util:get_test_db(),
    ?assertError(
        writes_not_allowed,
        erlfdb:transactional(Db1, fun(Tx) ->
            ?assert(erlfdb:get_writes_allowed(Tx)),

            erlfdb:set_option(Tx, disallow_writes),
            ?assert(not erlfdb:get_writes_allowed(Tx)),

            erlfdb:set_option(Tx, allow_writes),
            ?assert(erlfdb:get_writes_allowed(Tx)),

            erlfdb:set_option(Tx, disallow_writes),
            erlfdb:set(Tx, gen(10), gen(10))
        end)
    ).

once_writes_happend_cannot_disallow_them_test() ->
    Db1 = erlfdb_util:get_test_db(),
    ?assertError(
        badarg,
        erlfdb:transactional(Db1, fun(Tx) ->
            ok = erlfdb:set(Tx, gen(10), gen(10)),
            erlfdb:set_option(Tx, disallow_writes)
        end)
    ).

has_watches_test() ->
    Db1 = erlfdb_util:get_test_db(),
    {Before, After, AfterReset} = (erlfdb:transactional(Db1, fun(Tx) ->
        Before = erlfdb:has_watches(Tx),
        erlfdb:watch(Tx, gen(10)),
        After = erlfdb:has_watches(Tx),
        erlfdb:reset(Tx),
        AfterReset = erlfdb:has_watches(Tx),
        {Before, After, AfterReset}
    end)),
    ?assert(not Before),
    ?assert(After),
    ?assert(not AfterReset).

cannot_set_watches_if_writes_disallowed_test() ->
    Db1 = erlfdb_util:get_test_db(),
    ?assertError(
        writes_not_allowed,
        erlfdb:transactional(Db1, fun(Tx) ->
            erlfdb:set_option(Tx, disallow_writes),
            erlfdb:watch(Tx, gen(10))
        end)
    ).

size_limit_on_db_handle_test() ->
    Db1 = erlfdb_util:get_test_db(),
    erlfdb:set_option(Db1, size_limit, 10000),
    ?assertError(
        {erlfdb_error, 2101},
        erlfdb:transactional(Db1, fun(Tx) ->
            erlfdb:set(Tx, gen(10), gen(11000))
        end)
    ).

gen(Size) when is_integer(Size), Size > 1 ->
    RandBin = crypto:strong_rand_bytes(Size - 1),
    <<0, RandBin/binary>>.
