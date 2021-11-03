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

-module(erlfdb_key).

-export([
    to_selector/1,

    last_less_than/1,
    last_less_or_equal/1,
    first_greater_than/1,
    first_greater_or_equal/1,

    strinc/1
]).

to_selector(<<_/binary>> = Key) ->
    {Key, gteq};
to_selector({<<_/binary>>, _} = Sel) ->
    Sel;
to_selector({<<_/binary>>, _, _} = Sel) ->
    Sel;
to_selector(Else) ->
    erlang:error({invalid_key_selector, Else}).

last_less_than(Key) when is_binary(Key) ->
    {Key, lt}.

last_less_or_equal(Key) when is_binary(Key) ->
    {Key, lteq}.

first_greater_than(Key) when is_binary(Key) ->
    {Key, gt}.

first_greater_or_equal(Key) when is_binary(Key) ->
    {Key, gteq}.

strinc(Key) when is_binary(Key) ->
    Prefix = rstrip_ff(Key),
    PrefixLen = size(Prefix),
    Head = binary:part(Prefix, {0, PrefixLen - 1}),
    Tail = binary:at(Prefix, PrefixLen - 1),
    <<Head/binary, (Tail + 1)>>.

rstrip_ff(<<>>) ->
    erlang:error("Key must contain at least one byte not equal to 0xFF");
rstrip_ff(Key) ->
    KeyLen = size(Key),
    case binary:at(Key, KeyLen - 1) of
        16#FF -> rstrip_ff(binary:part(Key, {0, KeyLen - 1}));
        _ -> Key
    end.
