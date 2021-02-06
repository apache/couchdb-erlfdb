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

-module(erlfdb_error).

-export([
    error_name/1
]).

error_name(0) ->
    success;
error_name(1000) ->
    operation_failed;
error_name(1004) ->
    timed_out;
error_name(1007) ->
    transaction_too_old;
error_name(1009) ->
    future_version;
error_name(1020) ->
    not_committed;
error_name(1021) ->
    commit_unknown_result;
error_name(1025) ->
    transaction_cancelled;
error_name(1031) ->
    transaction_timed_out;
error_name(1032) ->
    too_many_watches;
error_name(1034) ->
    watches_disabled;
error_name(1036) ->
    accessed_unreadable;
error_name(1037) ->
    process_behind;
error_name(1038) ->
    database_locked;
error_name(1039) ->
    cluster_version_changed;
error_name(1040) ->
    external_client_already_loaded;
error_name(1042) ->
    proxy_memory_limit_exceeded;
error_name(1101) ->
    operation_cancelled;
error_name(1102) ->
    future_released;
error_name(1500) ->
    platform_error;
error_name(1501) ->
    large_alloc_failed;
error_name(1502) ->
    performance_counter_error;
error_name(1510) ->
    io_error;
error_name(1511) ->
    file_not_found;
error_name(1512) ->
    bind_failed;
error_name(1513) ->
    file_not_readable;
error_name(1514) ->
    file_not_writable;
error_name(1515) ->
    no_cluster_file_found;
error_name(1516) ->
    file_too_large;
error_name(2000) ->
    client_invalid_operation;
error_name(2002) ->
    commit_read_incomplete;
error_name(2003) ->
    test_specification_invalid;
error_name(2004) ->
    key_outside_legal_range;
error_name(2005) ->
    inverted_range;
error_name(2006) ->
    invalid_option_value;
error_name(2007) ->
    invalid_option;
error_name(2008) ->
    network_not_setup;
error_name(2009) ->
    network_already_setup;
error_name(2010) ->
    read_version_already_set;
error_name(2011) ->
    version_invalid;
error_name(2012) ->
    range_limits_invalid;
error_name(2013) ->
    invalid_database_name;
error_name(2014) ->
    attribute_not_found;
error_name(2015) ->
    future_not_set;
error_name(2016) ->
    future_not_error;
error_name(2017) ->
    used_during_commit;
error_name(2018) ->
    invalid_mutation_type;
error_name(2020) ->
    transaction_invalid_version;
error_name(2021) ->
    no_commit_version;
error_name(2022) ->
    environment_variable_network_option_failed;
error_name(2023) ->
    transaction_read_only;
error_name(2100) ->
    incompatible_protocol_version;
error_name(2101) ->
    transaction_too_large;
error_name(2102) ->
    key_too_large;
error_name(2103) ->
    value_too_large;
error_name(2104) ->
    connection_string_invalid;
error_name(2105) ->
    address_in_use;
error_name(2106) ->
    invalid_local_address;
error_name(2107) ->
    tls_error;
error_name(2108) ->
    unsupported_operation;
error_name(2200) ->
    api_version_unset;
error_name(2201) ->
    api_version_already_set;
error_name(2202) ->
    api_version_invalid;
error_name(2203) ->
    api_version_not_supported;
error_name(2210) ->
    exact_mode_without_limits;
error_name(4000) ->
    unknown_error;
error_name(4100) ->
    internal_error.
