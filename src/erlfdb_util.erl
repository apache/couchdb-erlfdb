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

-module(erlfdb_util).


-export([
    get/2,
    get/3,

    repr/1
]).



get(List, Key) ->
    get(List, Key, undefined).


get(List, Key, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Value} -> Value;
        _ -> Default
    end.


repr(Bin) when is_binary(Bin) ->
    [$'] ++ lists:map(fun(C) ->
        case C of
            9 -> "\\t";
            10 -> "\\n";
            13 -> "\\r";
            39 -> "\\'";
            92 -> "\\\\";
            _ when C >= 32, C =< 126 -> C;
            _ -> io_lib:format("\\x~2.16.0b", [C])
        end
    end, binary_to_list(Bin)) ++ [$'].
