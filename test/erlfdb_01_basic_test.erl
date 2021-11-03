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

-module(erlfdb_01_basic_test).

-include_lib("eunit/include/eunit.hrl").

load_test() ->
    erlfdb_nif:ohai().

get_error_string_test() ->
    ?assertEqual(<<"Success">>, erlfdb_nif:get_error(0)),
    ?assertEqual(
        <<"Transaction exceeds byte limit">>,
        erlfdb_nif:get_error(2101)
    ),
    ?assertEqual(<<"UNKNOWN_ERROR">>, erlfdb_nif:get_error(9999)).
