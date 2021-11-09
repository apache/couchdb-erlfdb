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

-module(erlfdb_subspace).

-record(erlfdb_subspace, {
    prefix
}).

-export([
    create/1,
    create/2,

    add/2,

    key/1,

    pack/1,
    pack/2,
    pack_vs/1,
    pack_vs/2,
    unpack/2,

    range/1,
    range/2,

    contains/2,

    subspace/2
]).

-define(PREFIX(S), S#erlfdb_subspace.prefix).

create(Tuple) ->
    create(Tuple, <<>>).

create(#erlfdb_subspace{} = Subspace, Tuple) when is_tuple(Tuple) ->
    create(Tuple, ?PREFIX(Subspace));
create(Tuple, Prefix) when is_tuple(Tuple), is_binary(Prefix) ->
    #erlfdb_subspace{
        prefix = erlfdb_tuple:pack(Tuple, Prefix)
    }.

add(#erlfdb_subspace{} = Subspace, Item) ->
    create({Item}, ?PREFIX(Subspace)).

key(#erlfdb_subspace{} = Subspace) ->
    Subspace#erlfdb_subspace.prefix.

pack(Subspace) ->
    pack(Subspace, {}).

pack(#erlfdb_subspace{} = Subspace, Tuple) when is_tuple(Tuple) ->
    erlfdb_tuple:pack(Tuple, ?PREFIX(Subspace)).

pack_vs(Subspace) ->
    pack_vs(Subspace, {}).

pack_vs(#erlfdb_subspace{} = Subspace, Tuple) when is_tuple(Tuple) ->
    erlfdb_tuple:pack_vs(Tuple, ?PREFIX(Subspace)).

unpack(#erlfdb_subspace{} = Subspace, Key) ->
    case contains(Subspace, Key) of
        true ->
            Prefix = ?PREFIX(Subspace),
            SubKey = binary:part(Key, size(Prefix), size(Key) - size(Prefix)),
            erlfdb_tuple:unpack(SubKey);
        false ->
            erlang:error({key_not_in_subspace, Subspace, Key})
    end.

range(Subspace) ->
    range(Subspace, {}).

range(#erlfdb_subspace{} = Subspace, Tuple) when is_tuple(Tuple) ->
    Prefix = ?PREFIX(Subspace),
    PrefixLen = size(Prefix),
    {Start, End} = erlfdb_tuple:range(Tuple),
    {
        <<Prefix:PrefixLen/binary, Start/binary>>,
        <<Prefix:PrefixLen/binary, End/binary>>
    }.

contains(#erlfdb_subspace{} = Subspace, Key) ->
    Prefix = ?PREFIX(Subspace),
    PrefLen = size(Prefix),
    case Key of
        <<Prefix:PrefLen/binary, _/binary>> ->
            true;
        _ ->
            false
    end.

subspace(#erlfdb_subspace{} = Subspace, Tuple) ->
    create(Subspace, Tuple).
