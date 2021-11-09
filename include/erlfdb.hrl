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
    erlfdb_subspace:range(erlfdb_subspace:create({}, Prefix))
).
-define(ERLFDB_RANGE(Prefix, Term),
    erlfdb_subspace:range(erlfdb_subspace:create({Term}, Prefix))
).

-define(ERLFDB_EXTEND(Prefix, Term), erlfdb_tuple:pack({Term}, Prefix)).

-define(ERLFDB_EXTRACT(Prefix, Packed),
    (fun() ->
        __PrefixLen = size(Prefix),
        <<Prefix:__PrefixLen/binary, __Tail/binary>> = Packed,
        erlfdb_tuple:unpack(__Tail)
    end)()
).

% Most of the retriable FDB errors. The list of errors can be generated with
% something like [erlfdb:get_error_string(C) || C <- lists:seq(1, 5000),
% erlfdb:error_predicate(retryable, C)].
%
-define(ERLFDB_TIMED_OUT, 1004).
-define(ERLFDB_TRANSACTION_TOO_OLD, 1007).
-define(ERLFDB_FUTURE_VERSION, 1009).
-define(ERLFDB_NOT_COMMITTED, 1020).
-define(ERLFDB_COMMIT_UNKNOWN_RESULT, 1021).
-define(ERLFDB_TRANSACTION_CANCELLED, 1025).
-define(ERLFDB_TRANSACTION_TIMED_OUT, 1031).
-define(ERLFDB_PROCESS_BEHIND, 1037).
-define(ERLFDB_DATABASE_LOCKED, 1038).
-define(ERLFDB_CLUSTER_VERSION_CHANGED, 1039).
-define(ERLFDB_PROXY_MEMORY_LIMIT_EXCEEDED, 1042).
-define(ERLFDB_BATCH_TRANSACTION_THROTTLED, 1051).
-define(ERLFDB_TAG_THROTTLED, 1213).
-define(ERLFDB_TRANSACTION_TOO_LARGE, 2101).

% The list is the exact result from calling `error_predicate/2` and does not
% include ?TRANSACTION_TIMED_OUT. In some cases it may make sense to also
% consider that error as retryable.
%
%% erlfmt-ignore
-define(ERLFDB_IS_RETRYABLE(Code), (
    (Code == ?ERLFDB_TRANSACTION_TOO_OLD) orelse
    (Code == ?ERLFDB_FUTURE_VERSION) orelse
    (Code == ?ERLFDB_NOT_COMMITTED) orelse
    (Code == ?ERLFDB_COMMIT_UNKNOWN_RESULT) orelse
    (Code == ?ERLFDB_PROCESS_BEHIND) orelse
    (Code == ?ERLFDB_DATABASE_LOCKED) orelse
    (Code == ?ERLFDB_CLUSTER_VERSION_CHANGED) orelse
    (Code == ?ERLFDB_PROXY_MEMORY_LIMIT_EXCEEDED) orelse
    (Code == ?ERLFDB_BATCH_TRANSACTION_THROTTLED) orelse
    (Code == ?ERLFDB_TAG_THROTTLED)
)).
