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

-module(erlfdb_hca).

% This is based on the FoundationDB Python bindings as well
% as this excellent blog post describing the logic:
%    https://ananthakumaran.in/2018/08/05/high-contention-allocator.html

-export([
    create/1,
    allocate/2
]).

-include("erlfdb.hrl").

-record(erlfdb_hca, {
    counters,
    recent
}).

create(Prefix) ->
    #erlfdb_hca{
        counters = ?ERLFDB_EXTEND(Prefix, 0),
        recent = ?ERLFDB_EXTEND(Prefix, 1)
    }.

allocate(HCA, Db) ->
    try
        erlfdb:transactional(Db, fun(Tx) ->
            Start = current_start(HCA, Tx),
            CandidateRange = range(HCA, Tx, Start, false),
            search_candidate(HCA, Tx, CandidateRange)
        end)
    catch
        throw:hca_retry ->
            allocate(HCA, Db)
    end.

current_start(HCA, Tx) ->
    #erlfdb_hca{
        counters = Counters
    } = HCA,
    {CRangeStart, CRangeEnd} = counter_range(Counters),
    Options = [
        {snapshot, true},
        {reverse, true},
        {streaming_mode, exact},
        {limit, 1}
    ],
    case erlfdb:wait(erlfdb:get_range(Tx, CRangeStart, CRangeEnd, Options)) of
        [] ->
            0;
        [{CounterKey, _}] ->
            {Start} = ?ERLFDB_EXTRACT(Counters, CounterKey),
            Start
    end.

range(HCA, Tx, Start, WindowAdvanced) ->
    #erlfdb_hca{
        counters = Counters
    } = HCA,

    if
        not WindowAdvanced -> ok;
        true -> clear_previous_window(HCA, Tx, Start)
    end,

    CounterKey = ?ERLFDB_EXTEND(Counters, Start),
    erlfdb:add(Tx, CounterKey, 1),

    Count =
        case erlfdb:wait(erlfdb:get_ss(Tx, CounterKey)) of
            <<C:64/little>> -> C;
            not_found -> 0
        end,

    WindowSize = window_size(Start),
    case Count * 2 < WindowSize of
        true -> {Start, WindowSize};
        false -> range(HCA, Tx, Start + WindowSize, true)
    end.

search_candidate(HCA, Tx, {Start, WindowSize}) ->
    #erlfdb_hca{
        counters = Counters,
        recent = Recent
    } = HCA,

    % -1 because random:uniform is 1 =< X $=< WindowSize
    Candidate = Start + rand:uniform(WindowSize) - 1,
    CandidateValueKey = ?ERLFDB_EXTEND(Recent, Candidate),

    Options = [
        {snapshot, true},
        {reverse, true},
        {streaming_mode, exact},
        {limit, 1}
    ],
    {CRangeStart, CRangeEnd} = counter_range(Counters),

    CFuture = erlfdb:get_range(Tx, CRangeStart, CRangeEnd, Options),
    CVFuture = erlfdb:get(Tx, CandidateValueKey),

    erlfdb:set_option(Tx, next_write_no_write_conflict_range),
    erlfdb:set(Tx, CandidateValueKey, <<>>),

    [LatestCounter, CandidateValue] = erlfdb:wait_for_all([CFuture, CVFuture]),

    % Check that we're still in the same counter window
    case LatestCounter of
        [{CounterKey, _}] ->
            {LStart} = ?ERLFDB_EXTRACT(Counters, CounterKey),
            if
                LStart == Start -> ok;
                true -> throw(hca_retry)
            end;
        _ ->
            ok
    end,

    case CandidateValue of
        not_found ->
            erlfdb:add_write_conflict_key(Tx, CandidateValueKey),
            erlfdb_tuple:pack({Candidate});
        _ ->
            throw(hca_retry)
    end.

clear_previous_window(HCA, Tx, Start) ->
    #erlfdb_hca{
        counters = Counters,
        recent = Recent
    } = HCA,

    {CRangeStart, CRangeEnd} = ?ERLFDB_RANGE(Counters, Start),
    {RRangeStart, RRangeEnd} = ?ERLFDB_RANGE(Recent, Start),

    erlfdb:clear_range(Tx, CRangeStart, CRangeEnd),
    erlfdb:set_option(Tx, next_write_no_write_conflict_range),
    erlfdb:clear_range(Tx, RRangeStart, RRangeEnd).

counter_range(Counters) ->
    S = erlfdb_subspace:create({}, Counters),
    erlfdb_subspace:range(S).

window_size(Start) ->
    if
        Start < 255 -> 64;
        Start < 65535 -> 1024;
        true -> 8192
    end.
