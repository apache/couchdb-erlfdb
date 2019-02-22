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


-define(ERLFDB_ERROR(Reason), erlang:error({?MODULE, Reason})).


-define(ERLFDB_PACK(Tuple), erlfdb_tuple:pack(Tuple)).
-define(ERLFDB_PACK(Prefix, Tuple), erlfdb_tuple:pack(Tuple, Prefix)).

-define(ERLFDB_RANGE(Prefix),
        erlfdb_subspace:range(erlfdb_subspace:create({}, Prefix))).
-define(ERLFDB_RANGE(Prefix, Term),
        erlfdb_subspace:range(erlfdb_subspace:create({Term}, Prefix))).

-define(ERLFDB_EXTEND(Prefix, Term), erlfdb_tuple:pack({Term}, Prefix)).

-define(ERLFDB_EXTRACT(Prefix, Packed), (fun() ->
        __PrefixLen = size(Prefix),
        <<Prefix:__PrefixLen/binary, __Tail/binary>> = Packed,
        erlfdb_tuple:unpack(__Tail)
    end)()).





