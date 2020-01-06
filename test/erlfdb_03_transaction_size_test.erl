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

-module(erlfdb_03_transaction_size_test).

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
    ?assertError({erlfdb_error, 2101}, erlfdb:transactional(Db1, fun(Tx) ->
         erlfdb:set_option(Tx, size_limit, 10000),
         erlfdb:set(Tx, gen(10), gen(11000))
    end)).


gen(Size) ->
    crypto:strong_rand_bytes(Size).
