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

-module(erlfdb_06_get_addresses_test).

-include_lib("eunit/include/eunit.hrl").

get_addresses_for_key_test() ->
    Db = erlfdb_util:get_test_db(),
    % Does not matter whether foo exists in Db
    Result = erlfdb:get_addresses_for_key(Db, <<"foo">>),
    ?assertMatch([X | _] when is_binary(X), Result).
