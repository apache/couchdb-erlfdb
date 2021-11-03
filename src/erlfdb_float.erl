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

-module(erlfdb_float).

-export([
    decode/1,
    encode/1
]).

decode(<<_Sign:1, 0:8, _Fraction:23>> = F) ->
    {float, denormalized, F};
decode(<<_Sign:1, 255:8, 0:23>> = F) ->
    {float, infinite, F};
decode(<<_Sign:1, 255:8, _Fraction:23>> = F) ->
    {float, nan, F};
decode(<<F:32/float>>) ->
    {float, F};
decode(<<_Sign:1, 0:11, _Fraction:52>> = D) ->
    {double, denormalized, D};
decode(<<_Sign:1, 2047:11, 0:52>> = D) ->
    {double, infinite, D};
decode(<<_Sign:1, 2047:11, _Fraction:52>> = D) ->
    {double, nan, D};
decode(<<D:64/float>>) ->
    D.

encode({float, F}) -> <<F:32/float>>;
encode({float, _Type, F}) -> F;
encode(F) when is_float(F) -> <<F:64/float>>;
encode({double, _Type, D}) -> D.
