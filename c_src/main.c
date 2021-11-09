// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "erl_nif.h"
#include "fdb.h"

#include "atoms.h"
#include "resources.h"
#include "util.h"


// FoundationDB can only be intiailized once
// in a given OS process. By creating a random
// atom we can flag whether erlfdb was previously
// initialized in a given Erlang VM.
const char* SENTINEL = "faaffadf46e64b87bdd0fedbddd97b1a";


typedef enum _ErlFDBLibState
{
    ErlFDB_STATE_ERROR = 0,
    ErlFDB_LOADED,
    ErlFDB_API_SELECTED,
    ErlFDB_NETWORK_STARTING,
    ErlFDB_CONNECTED,
} ErlFDBLibState;


typedef struct _ErlFDBSt
{
    ErlFDBLibState lib_state;
    ErlNifTid network_tid;
    ErlNifMutex* lock;
    ErlNifCond* cond;
} ErlFDBSt;


static void*
erlfdb_network_thread(void* arg)
{
    ErlFDBSt* st = (ErlFDBSt*) arg;
    fdb_error_t* err = enif_alloc(sizeof(fdb_error_t));

    enif_mutex_lock(st->lock);

    st->lib_state = ErlFDB_NETWORK_STARTING;

    enif_cond_signal(st->cond);
    enif_mutex_unlock(st->lock);

    *err = fdb_run_network();

    return (void*) err;
}


static void
erlfdb_future_cb(FDBFuture* fdb_future, void* data)
{
    ErlFDBFuture* future = (ErlFDBFuture*) data;
    ErlNifEnv* caller;
    ERL_NIF_TERM msg;

    // FoundationDB callbacks can fire from the thread
    // that created them. Check if we were actually
    // submitted to the network thread or not so that
    // we pass the correct environment to enif_send
    if(enif_thread_type() == ERL_NIF_THR_UNDEFINED) {
        caller = NULL;
    } else {
        caller = future->pid_env;
    }

    enif_mutex_lock(future->lock);

    if(!future->cancelled) {
        msg = T2(future->msg_env, future->msg_ref, ATOM_ready);
        enif_send(caller, &(future->pid), future->msg_env, msg);
    }

    enif_mutex_unlock(future->lock);

    // We're now done with this future which means we need
    // to release our handle to it. See erlfdb_create_future
    // for more on why this happens here.

    enif_release_resource(future);

    return;
}


static ERL_NIF_TERM
erlfdb_create_future(ErlNifEnv* env, FDBFuture* future, ErlFDBFutureType ftype)
{
    ErlFDBFuture* f;
    ERL_NIF_TERM ref = enif_make_ref(env);
    ERL_NIF_TERM ret;
    fdb_error_t err;

    f = enif_alloc_resource(ErlFDBFutureRes, sizeof(ErlFDBFuture));
    f->future = future;
    f->ftype = ftype;
    enif_self(env, &(f->pid));
    f->pid_env = env;
    f->msg_env = enif_alloc_env();
    f->msg_ref = enif_make_copy(f->msg_env, ref);
    f->lock = enif_mutex_create("fdb:future_lock");
    f->cancelled = false;

    // This resource reference counting dance is a bit
    // awkward as erlfdb_future_cb can be called both
    // synchronously and asynchronously.
    //
    // At this point the reference count is 1

    enif_keep_resource(f);

    // The reference count is now 2

    err = fdb_future_set_callback(
            f->future,
            erlfdb_future_cb,
            (void*) f
        );

    if(err != 0) {
        // If we failed to set the future callback function
        // then we assume the callback was not invoked and
        // we have to release twice
        enif_release_resource(f);
        enif_release_resource(f);
        return erlfdb_erlang_error(env, err);
    }

    // At this point our callback may have been called
    // which means we could have a reference count
    // of 1 or 2

    ret = enif_make_resource(env, f);

    // enif_make_resource increases the ref count
    // so now our reference count is either 2 or 3

    enif_release_resource(f);

    // Finally our reference count is now either
    // 1 or 2 depending on whether the callback
    // has already fired. If the ref count is
    // 2 then Erlang has a reference and the network
    // thread has a reference. If its 1 then only
    // Erlang has a reference.

    return T3(env, ATOM_erlfdb_future, ref, ret);
}


static inline ERL_NIF_TERM
erlfdb_future_get_void(ErlNifEnv* env, ErlFDBFuture* f)
{
    fdb_error_t err;

    err = fdb_future_get_error(f->future);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    return ATOM_ok;
}


static inline ERL_NIF_TERM
erlfdb_future_get_int64(ErlNifEnv* env, ErlFDBFuture* f)
{
    int64_t fdb_res;
    ErlNifSInt64 nif_res;
    fdb_error_t err;

    err = fdb_future_get_int64(f->future, &fdb_res);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    nif_res = fdb_res;

    return enif_make_int64(env, nif_res);
}


static inline ERL_NIF_TERM
erlfdb_future_get_key(ErlNifEnv* env, ErlFDBFuture* f)
{
    const uint8_t* key;
    int len;
    unsigned char* buf;
    ERL_NIF_TERM ret;
    fdb_error_t err;

    err = fdb_future_get_key(f->future, &key, &len);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    buf = enif_make_new_binary(env, len, &ret);
    memcpy(buf, key, len);

    return ret;
}


static inline ERL_NIF_TERM
erlfdb_future_get_value(ErlNifEnv* env, ErlFDBFuture* f)
{
    fdb_bool_t present;
    const uint8_t* val;
    int len;
    unsigned char* buf;
    ERL_NIF_TERM ret;
    fdb_error_t err;

    err = fdb_future_get_value(f->future, &present, &val, &len);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    if(!present) {
        return ATOM_not_found;
    }

    buf = enif_make_new_binary(env, len, &ret);
    memcpy(buf, val, len);

    return ret;
}


static inline ERL_NIF_TERM
erlfdb_future_get_string_array(ErlNifEnv* env, ErlFDBFuture* f)
{
    const char** strings;
    int count;
    unsigned char* buf;
    ERL_NIF_TERM bin;
    ERL_NIF_TERM ret;
    fdb_error_t err;
    int i;

    err = fdb_future_get_string_array(f->future, &strings, &count);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    ret = enif_make_list(env, 0);

    for(i = count - 1; i >= 0; i--) {
        buf = enif_make_new_binary(env, strlen(strings[i]), &bin);
        memcpy(buf, strings[i], strlen(strings[i]));
        ret = enif_make_list_cell(env, bin, ret);
    }

    return ret;
}


static inline ERL_NIF_TERM
erlfdb_future_get_keyvalue_array(ErlNifEnv* env, ErlFDBFuture* f)
{
    FDBKeyValue const* kvs;
    fdb_bool_t more;
    int count;
    unsigned char* buf;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ERL_NIF_TERM ret;
    fdb_error_t err;
    int i;

    err = fdb_future_get_keyvalue_array(f->future, &kvs, &count, &more);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    ret = enif_make_list(env, 0);

    for(i = count - 1; i >= 0; i--) {
        buf = enif_make_new_binary(env, kvs[i].key_length, &key);
        memcpy(buf, kvs[i].key, kvs[i].key_length);
        buf = enif_make_new_binary(env, kvs[i].value_length, &val);
        memcpy(buf, kvs[i].value, kvs[i].value_length);
        ret = enif_make_list_cell(env, T2(env, key, val), ret);
    }

    if(more) {
        return T3(env, ret, enif_make_int(env, count), ATOM_true);
    } else {
        return T3(env, ret, enif_make_int(env, count), ATOM_false);
    }
}


static int
erlfdb_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM num_schedulers)
{
    ErlFDBSt* st = (ErlFDBSt*) enif_alloc(sizeof(ErlFDBSt));

    erlfdb_init_atoms(env);

    if(!erlfdb_init_resources(env)) {
        return 1;
    }

    st->lock = enif_mutex_create("fdb:st_lock");
    st->cond = enif_cond_create("fdb:st_cond");

    st->lib_state = ErlFDB_LOADED;
    *priv = st;

    return 0;
}


static void
erlfdb_unload(ErlNifEnv* env, void* priv)
{
    ErlFDBSt* st = (ErlFDBSt*) priv;
    ErlFDBLibState lib_state = ErlFDB_STATE_ERROR;
    fdb_error_t err;
    void* tmp;
    fdb_error_t* net_err;
    int nif_err;

    enif_mutex_lock(st->lock);
    lib_state = st->lib_state;
    enif_mutex_unlock(st->lock);

    if(lib_state == ErlFDB_CONNECTED) {
        err = fdb_stop_network();
        assert(err == 0 && "Error disconnecting fdb client");

        nif_err = enif_thread_join(st->network_tid, &tmp);
        assert(nif_err == 0 && "Error joining network thread");

        net_err = (fdb_error_t*) tmp;
        assert(*net_err == 0 && "Error running network thread");
        enif_free(tmp);
    }

    enif_mutex_destroy(st->lock);
    enif_cond_destroy(st->cond);
    enif_free(priv);

    return;
}


static ERL_NIF_TERM
erlfdb_can_initialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ERL_NIF_TERM atom;

    if(st->lib_state != ErlFDB_LOADED) {
        return enif_make_badarg(env);
    }

    if(argc != 0) {
        return enif_make_badarg(env);
    }

    if(enif_make_existing_atom(env, SENTINEL, &atom, ERL_NIF_LATIN1)) {
        return ATOM_false;
    }

    enif_make_atom(env, SENTINEL);

    return ATOM_true;
}


static ERL_NIF_TERM
erlfdb_get_max_api_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int vsn = fdb_get_max_api_version();

    if(argc != 0) {
        return enif_make_badarg(env);
    }

    return enif_make_int(env, vsn);
}


static ERL_NIF_TERM
erlfdb_select_api_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    int vsn;
    fdb_error_t err;

    if(st->lib_state != ErlFDB_LOADED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[0], &vsn)) {
        return enif_make_badarg(env);
    }

    err = fdb_select_api_version(vsn);

    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    st->lib_state = ErlFDB_API_SELECTED;

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_network_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    FDBNetworkOption option;
    ErlNifBinary value;
    fdb_error_t err;

    if(st->lib_state != ErlFDB_API_SELECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(IS_ATOM(argv[0], local_address)) {
        option = FDB_NET_OPTION_LOCAL_ADDRESS;
    } else if(IS_ATOM(argv[0], cluster_file)) {
        option = FDB_NET_OPTION_CLUSTER_FILE;
    } else if(IS_ATOM(argv[0], trace_enable)) {
        option = FDB_NET_OPTION_TRACE_ENABLE;
    } else if(IS_ATOM(argv[0], trace_format)) {
        option = FDB_NET_OPTION_TRACE_FORMAT;
    } else if(IS_ATOM(argv[0], trace_roll_size)) {
        option = FDB_NET_OPTION_TRACE_ROLL_SIZE;
    } else if(IS_ATOM(argv[0], trace_max_logs_size)) {
        option = FDB_NET_OPTION_TRACE_MAX_LOGS_SIZE;
    } else if(IS_ATOM(argv[0], trace_log_group)) {
        option = FDB_NET_OPTION_TRACE_LOG_GROUP;
    } else if(IS_ATOM(argv[0], knob)) {
        option = FDB_NET_OPTION_KNOB;
    } else if(IS_ATOM(argv[0], tls_plugin)) {
        option = FDB_NET_OPTION_TLS_PLUGIN;
    } else if(IS_ATOM(argv[0], tls_cert_bytes)) {
        option = FDB_NET_OPTION_TLS_CERT_BYTES;
    } else if(IS_ATOM(argv[0], tls_cert_path)) {
        option = FDB_NET_OPTION_TLS_CERT_PATH;
    } else if(IS_ATOM(argv[0], tls_key_bytes)) {
        option = FDB_NET_OPTION_TLS_KEY_BYTES;
    } else if(IS_ATOM(argv[0], tls_key_path)) {
        option = FDB_NET_OPTION_TLS_KEY_PATH;
    } else if(IS_ATOM(argv[0], tls_verify_peers)) {
        option = FDB_NET_OPTION_TLS_VERIFY_PEERS;
    } else if(IS_ATOM(argv[0], client_buggify_enable)) {
        option = FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE;
    } else if(IS_ATOM(argv[0], client_buggify_disable)) {
        option = FDB_NET_OPTION_CLIENT_BUGGIFY_DISABLE;
    } else if(IS_ATOM(argv[0], client_buggify_section_activated_probability)) {
        option = FDB_NET_OPTION_CLIENT_BUGGIFY_SECTION_ACTIVATED_PROBABILITY;
    } else if(IS_ATOM(argv[0], client_buggify_section_fired_probability)) {
        option = FDB_NET_OPTION_CLIENT_BUGGIFY_SECTION_FIRED_PROBABILITY;
    } else if(IS_ATOM(argv[0], tls_ca_bytes)) {
        option = FDB_NET_OPTION_TLS_CA_BYTES;
    } else if(IS_ATOM(argv[0], tls_password)) {
        option = FDB_NET_OPTION_TLS_PASSWORD;
    } else if(IS_ATOM(argv[0], disable_multi_version_client_api)) {
        option = FDB_NET_OPTION_DISABLE_MULTI_VERSION_CLIENT_API;
    } else if(IS_ATOM(argv[0], callbacks_on_external_threads)) {
        option = FDB_NET_OPTION_CALLBACKS_ON_EXTERNAL_THREADS;
    } else if(IS_ATOM(argv[0], external_client_library)) {
        option = FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY;
    } else if(IS_ATOM(argv[0], external_client_directory)) {
        option = FDB_NET_OPTION_EXTERNAL_CLIENT_DIRECTORY;
    } else if(IS_ATOM(argv[0], disable_local_client)) {
        option = FDB_NET_OPTION_DISABLE_LOCAL_CLIENT;
    } else if(IS_ATOM(argv[0], disable_client_statistics_logging)) {
        option = FDB_NET_OPTION_DISABLE_CLIENT_STATISTICS_LOGGING;
    } else if(IS_ATOM(argv[0], enable_slow_task_profiling)) {
        option = FDB_NET_OPTION_ENABLE_SLOW_TASK_PROFILING;
    }
    #if FDB_API_VERSION >= 630
    else if(IS_ATOM(argv[0], enable_run_loop_profiling)) {
        option = FDB_NET_OPTION_ENABLE_RUN_LOOP_PROFILING;
    }
    #endif
    else {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }

    err = fdb_network_set_option(option, (uint8_t*) value.data, value.size);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_setup_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    fdb_error_t err;

    if(st->lib_state != ErlFDB_API_SELECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 0) {
        return enif_make_badarg(env);
    }

    err = fdb_setup_network();
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    if(enif_thread_create(
            "fdb:network_thread",
            &(st->network_tid),
            erlfdb_network_thread,
            (void*) st,
            NULL
        ) != 0) {
        return enif_make_badarg(env);
    }

    enif_mutex_lock(st->lock);

    while(st->lib_state != ErlFDB_NETWORK_STARTING) {
        enif_cond_wait(st->cond, st->lock);
    }

    enif_mutex_unlock(st->lock);

    st->lib_state = ErlFDB_CONNECTED;

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_future_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBFutureRes, &res)) {
        return enif_make_badarg(env);
    }
    future = (ErlFDBFuture*) res;

    enif_mutex_lock(future->lock);

    future->cancelled = true;
    fdb_future_cancel(future->future);

    enif_mutex_unlock(future->lock);

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_future_silence(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBFutureRes, &res)) {
        return enif_make_badarg(env);
    }
    future = (ErlFDBFuture*) res;

    enif_mutex_lock(future->lock);

    future->cancelled = true;

    enif_mutex_unlock(future->lock);

    return ATOM_ok;
}

static ERL_NIF_TERM
erlfdb_future_is_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBFutureRes, &res)) {
        return enif_make_badarg(env);
    }
    future = (ErlFDBFuture*) res;

    if(fdb_future_is_ready(future->future)) {
        return ATOM_true;
    } else {
        return ATOM_false;
    }
}


static ERL_NIF_TERM
erlfdb_future_get_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBFuture* future;
    fdb_error_t err;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBFutureRes, &res)) {
        return enif_make_badarg(env);
    }
    future = (ErlFDBFuture*) res;

    err = fdb_future_get_error(future->future);

    return T2(env, ATOM_erlfdb_error, enif_make_int(env, err));
}


static ERL_NIF_TERM
erlfdb_future_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBFuture* f;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBFutureRes, &res)) {
        return enif_make_badarg(env);
    }
    f = (ErlFDBFuture*) res;

    if(f->ftype == ErlFDB_FT_VOID) {
        return erlfdb_future_get_void(env, f);
    } else if(f->ftype == ErlFDB_FT_INT64) {
        return erlfdb_future_get_int64(env, f);
    } else if(f->ftype == ErlFDB_FT_KEY) {
        return erlfdb_future_get_key(env, f);
    } else if(f->ftype == ErlFDB_FT_VALUE) {
        return erlfdb_future_get_value(env, f);
    } else if(f->ftype == ErlFDB_FT_STRING_ARRAY) {
        return erlfdb_future_get_string_array(env, f);
    } else if(f->ftype == ErlFDB_FT_KEYVALUE_ARRAY) {
        return erlfdb_future_get_keyvalue_array(env, f);
    }

    return enif_raise_exception(env, ATOM_invalid_future_type);
}


static ERL_NIF_TERM
erlfdb_create_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlNifBinary bin;
    FDBDatabase* database;
    fdb_error_t err;
    ErlFDBDatabase* d;
    ERL_NIF_TERM ret;


    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    if(bin.size < 1 || bin.data[bin.size - 1] != 0) {
        return enif_make_badarg(env);
    }

    err = fdb_create_database((const char*) bin.data, &database);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    d = enif_alloc_resource(ErlFDBDatabaseRes, sizeof(ErlFDBDatabase));
    d->database = database;

    ret = enif_make_resource(env, d);
    enif_release_resource(d);

    return T2(env, ATOM_erlfdb_database, ret);
}


static ERL_NIF_TERM
erlfdb_database_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBDatabase* d;
    FDBDatabaseOption option;
    ErlNifBinary value;
    fdb_error_t err;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBDatabaseRes, &res)) {
        return enif_make_badarg(env);
    }
    d = (ErlFDBDatabase*) res;

    if(IS_ATOM(argv[1], location_cache_size)) {
        option = FDB_DB_OPTION_LOCATION_CACHE_SIZE;
    } else if(IS_ATOM(argv[1], max_watches)) {
        option = FDB_DB_OPTION_MAX_WATCHES;
    } else if(IS_ATOM(argv[1], machine_id)) {
        option = FDB_DB_OPTION_MACHINE_ID;
    } else if(IS_ATOM(argv[1], datacenter_id)) {
        option = FDB_DB_OPTION_DATACENTER_ID;
    } else if(IS_ATOM(argv[1], read_your_writes_enable)) {
        option = FDB_DB_OPTION_SNAPSHOT_RYW_ENABLE;
    } else if(IS_ATOM(argv[1], read_your_writes_disable)) {
        option = FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE;
    } else if(IS_ATOM(argv[1], transaction_logging_max_field_length)) {
        option = FDB_DB_OPTION_TRANSACTION_LOGGING_MAX_FIELD_LENGTH;
    } else if(IS_ATOM(argv[1], timeout)) {
        option = FDB_DB_OPTION_TRANSACTION_TIMEOUT;
    } else if(IS_ATOM(argv[1], retry_limit)) {
        option = FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT;
    } else if(IS_ATOM(argv[1], max_retry_delay)) {
        option = FDB_DB_OPTION_TRANSACTION_MAX_RETRY_DELAY;
    } else if(IS_ATOM(argv[1], size_limit)) {
        option = FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT;
    } else if(IS_ATOM(argv[1], causal_read_risky)) {
        option = FDB_DB_OPTION_TRANSACTION_CAUSAL_READ_RISKY;
    } else if(IS_ATOM(argv[1], include_port_in_address)) {
        option = FDB_DB_OPTION_TRANSACTION_INCLUDE_PORT_IN_ADDRESS;
    } else {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    err = fdb_database_set_option(
            d->database,
            option,
            (uint8_t*) value.data,
            value.size
        );

    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_database_create_transaction(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBDatabase* d;
    ErlFDBTransaction* t;
    FDBTransaction* transaction;
    ErlNifPid pid;
    ERL_NIF_TERM ret;
    void* res;
    fdb_error_t err;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBDatabaseRes, &res)) {
        return enif_make_badarg(env);
    }
    d = (ErlFDBDatabase*) res;

    err = fdb_database_create_transaction(d->database, &transaction);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    t = enif_alloc_resource(ErlFDBTransactionRes, sizeof(ErlFDBTransaction));
    t->transaction = transaction;

    enif_self(env, &pid);
    t->owner = enif_make_pid(env, &pid);

    t->txid = 0;
    t->read_only = true;
    t->writes_allowed = true;
    t->has_watches = false;

    ret = enif_make_resource(env, t);
    enif_release_resource(t);
    return T2(env, ATOM_erlfdb_transaction, ret);
}


static ERL_NIF_TERM
erlfdb_transaction_set_option(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    FDBTransactionOption option;
    ErlNifBinary value;
    fdb_error_t err;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(IS_ATOM(argv[1], allow_writes)) {
        t->writes_allowed = true;
        return ATOM_ok;
    } else if (IS_ATOM(argv[1], disallow_writes)) {
        if(!t->read_only) {
            return enif_make_badarg(env);
        }
        t->writes_allowed = false;
        return ATOM_ok;
    }


    if(IS_ATOM(argv[1], causal_write_risky)) {
        option = FDB_TR_OPTION_CAUSAL_WRITE_RISKY;
    } else if(IS_ATOM(argv[1], causal_read_risky)) {
        option = FDB_TR_OPTION_CAUSAL_READ_RISKY;
    } else if(IS_ATOM(argv[1], causal_read_disable)) {
        option = FDB_TR_OPTION_CAUSAL_READ_DISABLE;
    } else if(IS_ATOM(argv[1], include_port_in_address)) {
        option = FDB_TR_OPTION_INCLUDE_PORT_IN_ADDRESS;
    } else if(IS_ATOM(argv[1], next_write_no_write_conflict_range)) {
        option = FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE;
    } else if(IS_ATOM(argv[1], read_your_writes_disable)) {
        option = FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE;
    } else if(IS_ATOM(argv[1], read_ahead_disable)) {
        option = FDB_TR_OPTION_READ_AHEAD_DISABLE;
    } else if(IS_ATOM(argv[1], durability_datacenter)) {
        option = FDB_TR_OPTION_DURABILITY_DATACENTER;
    } else if(IS_ATOM(argv[1], durability_risky)) {
        option = FDB_TR_OPTION_DURABILITY_RISKY;
    } else if(IS_ATOM(argv[1], durability_dev_null_is_web_scale)) {
        option = FDB_TR_OPTION_DURABILITY_DEV_NULL_IS_WEB_SCALE;
    } else if(IS_ATOM(argv[1], priority_system_immediate)) {
        option = FDB_TR_OPTION_PRIORITY_SYSTEM_IMMEDIATE;
    } else if(IS_ATOM(argv[1], priority_batch)) {
        option = FDB_TR_OPTION_PRIORITY_BATCH;
    } else if(IS_ATOM(argv[1], initialize_new_database)) {
        option = FDB_TR_OPTION_INITIALIZE_NEW_DATABASE;
    } else if(IS_ATOM(argv[1], access_system_keys)) {
        option = FDB_TR_OPTION_ACCESS_SYSTEM_KEYS;
    } else if(IS_ATOM(argv[1], read_system_keys)) {
        option = FDB_TR_OPTION_READ_SYSTEM_KEYS;
    } else if(IS_ATOM(argv[1], debug_retry_logging)) {
        option = FDB_TR_OPTION_DEBUG_RETRY_LOGGING;
    } else if(IS_ATOM(argv[1], transaction_logging_enable)) {
        option = FDB_TR_OPTION_TRANSACTION_LOGGING_ENABLE;
    } else if(IS_ATOM(argv[1], debug_transaction_identifier)) {
        option = FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER;
    } else if(IS_ATOM(argv[1], log_transaction)) {
        option = FDB_TR_OPTION_LOG_TRANSACTION;
    } else if(IS_ATOM(argv[1], transaction_logging_max_field_length)) {
        option = FDB_TR_OPTION_TRANSACTION_LOGGING_MAX_FIELD_LENGTH;
    } else if(IS_ATOM(argv[1], timeout)) {
        option = FDB_TR_OPTION_TIMEOUT;
    } else if(IS_ATOM(argv[1], retry_limit)) {
        option = FDB_TR_OPTION_RETRY_LIMIT;
    } else if(IS_ATOM(argv[1], max_retry_delay)) {
        option = FDB_TR_OPTION_MAX_RETRY_DELAY;
    } else if(IS_ATOM(argv[1], snapshot_ryw_enable)) {
        option = FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE;
    } else if(IS_ATOM(argv[1], snapshot_ryw_disable)) {
        option = FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE;
    } else if(IS_ATOM(argv[1], lock_aware)) {
        option = FDB_TR_OPTION_LOCK_AWARE;
    } else if(IS_ATOM(argv[1], used_during_commit_protection_disable)) {
        option = FDB_TR_OPTION_USED_DURING_COMMIT_PROTECTION_DISABLE;
    } else if(IS_ATOM(argv[1], read_lock_aware)) {
        option = FDB_TR_OPTION_READ_LOCK_AWARE;
    } else if(IS_ATOM(argv[1], size_limit)) {
        option = FDB_TR_OPTION_SIZE_LIMIT;
    } else if(IS_ATOM(argv[1], use_provisional_proxies)) {
        option = FDB_TR_OPTION_USE_PROVISIONAL_PROXIES;
#if FDB_API_VERSION > 620
    } else if(IS_ATOM(argv[1], report_conflicting_keys)) {
        option = FDB_TR_OPTION_REPORT_CONFLICTING_KEYS;
#endif
    } else {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    err = fdb_transaction_set_option(
            t->transaction,
            option,
            (uint8_t*) value.data,
            value.size
        );

    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_set_read_version(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifSInt64 erl_vsn;
    int64_t fdb_vsn;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int64(env, argv[1], &erl_vsn)) {
        return enif_make_badarg(env);
    }

    fdb_vsn = erl_vsn;

    fdb_transaction_set_read_version(t->transaction, fdb_vsn);

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_get_read_version(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_read_version(t->transaction);

    return erlfdb_create_future(env, future, ErlFDB_FT_INT64);
}


static ERL_NIF_TERM
erlfdb_transaction_get(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    fdb_bool_t snapshot;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    if(!erlfdb_get_boolean(argv[2], &snapshot)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get(
            t->transaction,
            (uint8_t*) key.data,
            key.size,
            snapshot
        );

    return erlfdb_create_future(env, future, ErlFDB_FT_VALUE);
}

#if FDB_API_VERSION >= 630
static ERL_NIF_TERM
erlfdb_transaction_get_estimated_range_size(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary skey;
    ErlNifBinary ekey;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[1], &skey)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &ekey)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_estimated_range_size_bytes(
            t->transaction,
            (uint8_t*) skey.data,
            skey.size,
            (uint8_t*) ekey.data,
            ekey.size
        );

    return erlfdb_create_future(env, future, ErlFDB_FT_INT64);
}
#endif

static ERL_NIF_TERM
erlfdb_transaction_get_key(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    fdb_bool_t or_equal;
    int offset;
    fdb_bool_t snapshot;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!erlfdb_get_key_selector(env, argv[1], &key, &or_equal, &offset)) {
        return enif_make_badarg(env);
    }

    if(!erlfdb_get_boolean(argv[2], &snapshot)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_key(
            t->transaction,
            (uint8_t*) key.data,
            key.size,
            or_equal,
            offset,
            snapshot
        );

    return erlfdb_create_future(env, future, ErlFDB_FT_KEY);
}


static ERL_NIF_TERM
erlfdb_transaction_get_addresses_for_key(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_addresses_for_key(
            t->transaction,
            (uint8_t*) key.data,
            key.size
        );

    return erlfdb_create_future(env, future, ErlFDB_FT_STRING_ARRAY);
}

static ERL_NIF_TERM
erlfdb_transaction_get_range(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;

    ErlNifBinary skey;
    fdb_bool_t sor_equal;
    int soffset;

    ErlNifBinary ekey;
    fdb_bool_t eor_equal;
    int eoffset;

    int limit;
    int target_bytes;
    FDBStreamingMode mode;
    int iteration;
    fdb_bool_t snapshot;
    int reverse;

    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 9) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!erlfdb_get_key_selector(env, argv[1], &skey, &sor_equal, &soffset)) {
        return enif_make_badarg(env);
    }

    if(!erlfdb_get_key_selector(env, argv[2], &ekey, &eor_equal, &eoffset)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[3], &limit)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[4], &target_bytes)) {
        return enif_make_badarg(env);
    }

    if(IS_ATOM(argv[5], want_all)) {
        mode = FDB_STREAMING_MODE_WANT_ALL;
    } else if(IS_ATOM(argv[5], iterator)) {
        mode = FDB_STREAMING_MODE_ITERATOR;
    } else if(IS_ATOM(argv[5], exact)) {
        mode = FDB_STREAMING_MODE_EXACT;
    } else if(IS_ATOM(argv[5], small)) {
        mode = FDB_STREAMING_MODE_SMALL;
    } else if(IS_ATOM(argv[5], medium)) {
        mode = FDB_STREAMING_MODE_MEDIUM;
    } else if(IS_ATOM(argv[5], large)) {
        mode = FDB_STREAMING_MODE_LARGE;
    } else if(IS_ATOM(argv[5], serial)) {
        mode = FDB_STREAMING_MODE_SERIAL;
    } else if(!enif_get_int(env, argv[5], &mode)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[6], &iteration)) {
        return enif_make_badarg(env);
    }

    if(!erlfdb_get_boolean(argv[7], &snapshot)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[8], &reverse)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_range(
            t->transaction,
            (uint8_t*) skey.data,
            skey.size,
            sor_equal,
            soffset,
            (uint8_t*) ekey.data,
            ekey.size,
            eor_equal,
            eoffset,
            limit,
            target_bytes,
            mode,
            iteration,
            snapshot,
            reverse
        );

    return erlfdb_create_future(env, future, ErlFDB_FT_KEYVALUE_ARRAY);
}


static ERL_NIF_TERM
erlfdb_transaction_set(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    ErlNifBinary val;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!t->writes_allowed) {
        return enif_raise_exception(env, ATOM_writes_not_allowed);
    }

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &val)) {
        return enif_make_badarg(env);
    }

    fdb_transaction_set(
            t->transaction,
            (uint8_t*) key.data,
            key.size,
            (uint8_t*) val.data,
            val.size
        );

    t->read_only = false;

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_clear(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!t->writes_allowed) {
        return enif_raise_exception(env, ATOM_writes_not_allowed);
    }

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    fdb_transaction_clear(t->transaction, (uint8_t*) key.data, key.size);

    t->read_only = false;

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_clear_range(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary skey;
    ErlNifBinary ekey;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!t->writes_allowed) {
        return enif_raise_exception(env, ATOM_writes_not_allowed);
    }

    if(!enif_inspect_binary(env, argv[1], &skey)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &ekey)) {
        return enif_make_badarg(env);
    }

    fdb_transaction_clear_range(
            t->transaction,
            (uint8_t*) skey.data,
            skey.size,
            (uint8_t*) ekey.data,
            ekey.size
        );

    t->read_only = false;

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_atomic_op(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    FDBMutationType mtype;
    ErlNifBinary param;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 4) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!t->writes_allowed) {
        return enif_raise_exception(env, ATOM_writes_not_allowed);
    }

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &param)) {
        return enif_make_badarg(env);
    }

    if(IS_ATOM(argv[3], add)) {
        mtype = FDB_MUTATION_TYPE_ADD;
    } else if(IS_ATOM(argv[3], bit_and)) {
        mtype = FDB_MUTATION_TYPE_BIT_AND;
    } else if(IS_ATOM(argv[3], bit_or)) {
        mtype = FDB_MUTATION_TYPE_BIT_OR;
    } else if(IS_ATOM(argv[3], bit_xor)) {
        mtype = FDB_MUTATION_TYPE_BIT_XOR;
    } else if(IS_ATOM(argv[3], append_if_fits)) {
        mtype = FDB_MUTATION_TYPE_APPEND_IF_FITS;
    } else if(IS_ATOM(argv[3], max)) {
        mtype = FDB_MUTATION_TYPE_MAX;
    } else if(IS_ATOM(argv[3], min)) {
        mtype = FDB_MUTATION_TYPE_MIN;
    } else if(IS_ATOM(argv[3], byte_min)) {
        mtype = FDB_MUTATION_TYPE_BYTE_MIN;
    } else if(IS_ATOM(argv[3], byte_max)) {
        mtype = FDB_MUTATION_TYPE_BYTE_MAX;
    } else if(IS_ATOM(argv[3], set_versionstamped_key)) {
        mtype = FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY;
    } else if(IS_ATOM(argv[3], set_versionstamped_value)) {
        mtype = FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE;
    } else {
        return enif_make_badarg(env);
    }

    fdb_transaction_atomic_op(
            t->transaction,
            (uint8_t*) key.data,
            key.size,
            (uint8_t*) param.data,
            param.size,
            mtype
        );

    t->read_only = false;

    return ATOM_ok;
}

static ERL_NIF_TERM
erlfdb_transaction_commit(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_commit(t->transaction);

    return erlfdb_create_future(env, future, ErlFDB_FT_VOID);
}


static ERL_NIF_TERM
erlfdb_transaction_get_committed_version(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    int64_t fdb_vsn;
    ErlNifSInt64 erl_vsn;
    fdb_error_t err;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    err = fdb_transaction_get_committed_version(t->transaction, &fdb_vsn);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    erl_vsn = fdb_vsn;
    return enif_make_int64(env, erl_vsn);
}


static ERL_NIF_TERM
erlfdb_transaction_get_versionstamp(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_versionstamp(t->transaction);

    return erlfdb_create_future(env, future, ErlFDB_FT_KEY);
}


static ERL_NIF_TERM
erlfdb_transaction_watch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary key;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    // In order for the watches to fire the transaction must commit, even if it
    // is a read-only transaction. So if writes are explicitly disallowed, also
    // do not allow setting any watches.
    if(!t->writes_allowed) {
        return enif_raise_exception(env, ATOM_writes_not_allowed);
    }

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_watch(
            t->transaction,
            (uint8_t*) key.data,
            key.size
        );

    t->has_watches = true;
    return erlfdb_create_future(env, future, ErlFDB_FT_VOID);
}


static ERL_NIF_TERM
erlfdb_transaction_on_error(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    int erl_err;
    fdb_error_t fdb_err;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &erl_err)) {
        return enif_make_badarg(env);
    }

    fdb_err = erl_err;

    future = fdb_transaction_on_error(t->transaction, fdb_err);

    return erlfdb_create_future(env, future, ErlFDB_FT_VOID);
}


static ERL_NIF_TERM
erlfdb_transaction_reset(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    fdb_transaction_reset(t->transaction);

    t->txid = 0;
    t->read_only = true;
    t->has_watches = false;

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_cancel(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    fdb_transaction_cancel(t->transaction);

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_add_conflict_range(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    ErlNifBinary skey;
    ErlNifBinary ekey;
    FDBConflictRangeType rtype;
    fdb_error_t err;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 4) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[1], &skey)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_binary(env, argv[2], &ekey)) {
        return enif_make_badarg(env);
    }

    if(IS_ATOM(argv[3], read)) {
        rtype = FDB_CONFLICT_RANGE_TYPE_READ;
    } else if(IS_ATOM(argv[3], write)) {
        if(!t->writes_allowed) {
            return enif_raise_exception(env, ATOM_writes_not_allowed);
        }
        rtype = FDB_CONFLICT_RANGE_TYPE_WRITE;
    } else {
        return enif_make_badarg(env);
    }

    err = fdb_transaction_add_conflict_range(
            t->transaction,
            (uint8_t*) skey.data,
            skey.size,
            (uint8_t*) ekey.data,
            ekey.size,
            rtype
        );

    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    if(rtype == FDB_CONFLICT_RANGE_TYPE_WRITE) {
        t->read_only = false;
    }

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_get_approximate_size(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    FDBFuture* future;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get_approximate_size(t->transaction);

    return erlfdb_create_future(env, future, ErlFDB_FT_INT64);
}


static ERL_NIF_TERM
erlfdb_transaction_get_next_tx_id(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(t->txid > 65535) {
        return enif_make_badarg(env);
    }

    return enif_make_uint(env, t->txid++);
}


static ERL_NIF_TERM
erlfdb_transaction_is_read_only(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(t->read_only) {
        return ATOM_true;
    } else {
        return ATOM_false;
    }
}


static ERL_NIF_TERM
erlfdb_transaction_has_watches(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(t->has_watches) {
        return ATOM_true;
    } else {
        return ATOM_false;
    }
}


static ERL_NIF_TERM
erlfdb_transaction_get_writes_allowed(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[]
    )
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBTransaction* t;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBTransactionRes, &res)) {
        return enif_make_badarg(env);
    }
    t = (ErlFDBTransaction*) res;

    if(!erlfdb_transaction_is_owner(env, t)) {
        return enif_make_badarg(env);
    }

    if(t->writes_allowed) {
        return ATOM_true;
    } else {
        return ATOM_false;
    }
}


static ERL_NIF_TERM
erlfdb_get_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int erl_err;
    fdb_error_t fdb_err;
    const char* str_err;
    ERL_NIF_TERM ret;
    unsigned char* buf;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[0], &erl_err)) {
        return enif_make_badarg(env);
    }

    fdb_err = erl_err;
    str_err = fdb_get_error(fdb_err);

    // No clue if this happens ever
    if(!str_err) {
        return enif_make_badarg(env);
    }

    buf = enif_make_new_binary(env, strlen(str_err), &ret);
    memcpy(buf, str_err, strlen(str_err));

    return ret;
}


static ERL_NIF_TERM
erlfdb_error_predicate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int erl_err;
    fdb_error_t fdb_err;
    FDBErrorPredicate pred;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(IS_ATOM(argv[0], retryable)) {
        pred = FDB_ERROR_PREDICATE_RETRYABLE;
    } else if(IS_ATOM(argv[0], maybe_committed)) {
        pred = FDB_ERROR_PREDICATE_MAYBE_COMMITTED;
    } else if(IS_ATOM(argv[0], retryable_not_committed)) {
        pred = FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED;
    } else{
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &erl_err)) {
        return enif_make_badarg(env);
    }

    fdb_err = erl_err;

    if(fdb_error_predicate(pred, fdb_err)) {
        return ATOM_true;
    } else {
        return ATOM_false;
    }
}


#define NIF_FUNC(name, arity) {#name, arity, name}
static ErlNifFunc funcs[] =
{
    NIF_FUNC(erlfdb_can_initialize, 0),

    NIF_FUNC(erlfdb_get_max_api_version, 0),
    NIF_FUNC(erlfdb_select_api_version, 1),

    NIF_FUNC(erlfdb_network_set_option, 2),
    NIF_FUNC(erlfdb_setup_network, 0),

    NIF_FUNC(erlfdb_future_cancel, 1),
    NIF_FUNC(erlfdb_future_silence, 1),
    NIF_FUNC(erlfdb_future_is_ready, 1),
    NIF_FUNC(erlfdb_future_get_error, 1),
    NIF_FUNC(erlfdb_future_get, 1),

    NIF_FUNC(erlfdb_create_database, 1),
    NIF_FUNC(erlfdb_database_set_option, 3),
    NIF_FUNC(erlfdb_database_create_transaction, 1),

    NIF_FUNC(erlfdb_transaction_set_option, 3),
    NIF_FUNC(erlfdb_transaction_set_read_version, 2),
    NIF_FUNC(erlfdb_transaction_get_read_version, 1),
    NIF_FUNC(erlfdb_transaction_get, 3),
    NIF_FUNC(erlfdb_transaction_get_key, 3),
    NIF_FUNC(erlfdb_transaction_get_addresses_for_key, 2),
    NIF_FUNC(erlfdb_transaction_get_range, 9),
    NIF_FUNC(erlfdb_transaction_set, 3),
    NIF_FUNC(erlfdb_transaction_clear, 2),
    NIF_FUNC(erlfdb_transaction_clear_range, 3),
    NIF_FUNC(erlfdb_transaction_atomic_op, 4),
    NIF_FUNC(erlfdb_transaction_commit, 1),
    NIF_FUNC(erlfdb_transaction_get_committed_version, 1),
    NIF_FUNC(erlfdb_transaction_get_versionstamp, 1),
    NIF_FUNC(erlfdb_transaction_watch, 2),
    NIF_FUNC(erlfdb_transaction_on_error, 2),
    NIF_FUNC(erlfdb_transaction_reset, 1),
    NIF_FUNC(erlfdb_transaction_cancel, 1),
    NIF_FUNC(erlfdb_transaction_add_conflict_range, 4),
    NIF_FUNC(erlfdb_transaction_get_approximate_size, 1),
    NIF_FUNC(erlfdb_transaction_get_next_tx_id, 1),
    NIF_FUNC(erlfdb_transaction_is_read_only, 1),
    NIF_FUNC(erlfdb_transaction_has_watches, 1),
    NIF_FUNC(erlfdb_transaction_get_writes_allowed, 1),

    #if FDB_API_VERSION >= 630
    NIF_FUNC(erlfdb_transaction_get_estimated_range_size, 3),
    #endif

    NIF_FUNC(erlfdb_get_error, 1),
    NIF_FUNC(erlfdb_error_predicate, 2)
};
#undef NIF_FUNC


ERL_NIF_INIT(erlfdb_nif, funcs, &erlfdb_load, NULL, NULL, &erlfdb_unload);
