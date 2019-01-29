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

    msg = T2(future->msg_env, ATOM_ready, future->msg_ref);
    enif_send(caller, &(future->pid), future->msg_env, msg);

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

    err = fdb_future_set_callback(
            f->future, erlfdb_future_cb, (void*) future);

    if(err != 0) {
        enif_release_resource(f);
        return erlfdb_erlang_error(env, err);
    }

    ret = enif_make_resource(env, f);
    return T2(env, ATOM_ok, T3(env, ATOM_erlfdb_future, ref, ret));
}


static inline ERL_NIF_TERM
erlfdb_future_get_version(ErlNifEnv* env, ErlFDBFuture* f)
{
    int64_t fdb_vsn;
    ErlNifSInt64 nif_vsn;
    fdb_error_t err;

    err = fdb_future_get_version(f->future, &fdb_vsn);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    nif_vsn = fdb_vsn;

    return T2(env, ATOM_ok, enif_make_int64(env, nif_vsn));
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

    return T2(env, ATOM_ok, ret);
}


static inline ERL_NIF_TERM
erlfdb_future_get_cluster(ErlNifEnv* env, ErlFDBFuture* f)
{
    FDBCluster* cluster;
    ErlFDBCluster* c;
    ERL_NIF_TERM ret;
    fdb_error_t err;

    err = fdb_future_get_cluster(f->future, &cluster);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    c = enif_alloc_resource(ErlFDBClusterRes, sizeof(ErlFDBCluster));
    c->cluster = cluster;

    ret = enif_make_resource(env, c);
    enif_release_resource(c);

    return T2(env, ATOM_ok, T2(env, ATOM_erlfdb_cluster, ret));
}


static inline ERL_NIF_TERM
erlfdb_future_get_database(ErlNifEnv* env, ErlFDBFuture* f)
{
    FDBDatabase* database;
    ErlFDBDatabase* d;
    ERL_NIF_TERM ret;
    fdb_error_t err;

    err = fdb_future_get_database(f->future, &database);
    if(err != 0) {
        return erlfdb_erlang_error(env, err);
    }

    d = enif_alloc_resource(ErlFDBDatabaseRes, sizeof(ErlFDBDatabase));
    d->database = database;

    ret = enif_make_resource(env, d);
    enif_release_resource(d);

    return T2(env, ATOM_ok, T2(env, ATOM_erlfdb_database, ret));
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
        return T2(env, ATOM_ok, ATOM_not_found);
    }

    buf = enif_make_new_binary(env, len, &ret);
    memcpy(buf, val, len);

    return T2(env, ATOM_ok, ret);
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

    return T2(env, ATOM_ok, enif_make_int(env, vsn));
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

    if(st->lib_state != ErlFDB_API_SELECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    return ATOM_error;
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

    if(enif_thread_create("fdb:network_thread", &(st->network_tid),
            erlfdb_network_thread, (void*) st, NULL) != 0) {
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

    fdb_future_cancel(future->future);

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

    return erlfdb_erlang_error(env, err);
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

    if(f->ftype == ErlFDB_FT_VERSION) {
        return erlfdb_future_get_version(env, f);
    } else if(f->ftype == ErlFDB_FT_KEY) {
        return erlfdb_future_get_key(env, f);
    } else if(f->ftype == ErlFDB_FT_CLUSTER) {
        return erlfdb_future_get_cluster(env, f);
    } else if(f->ftype == ErlFDB_FT_DATABASE) {
        return erlfdb_future_get_database(env, f);
    } else if(f->ftype == ErlFDB_FT_VALUE) {
        return erlfdb_future_get_value(env, f);
    }

    return T2(env, ATOM_error, ATOM_invalid_future_type);
}


static ERL_NIF_TERM
erlfdb_create_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    FDBFuture* future;
    ErlNifBinary bin;

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

    future = fdb_create_cluster((const char*) bin.data);

    return erlfdb_create_future(env, future, ErlFDB_FT_CLUSTER);
}


static ERL_NIF_TERM
erlfdb_cluster_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // There are currently no valid cluster options
    return enif_make_badarg(env);
}


static ERL_NIF_TERM
erlfdb_cluster_create_database(
        ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBCluster* c;
    FDBFuture* future;
    ErlNifBinary bin;
    void* res;

    if(st->lib_state != ErlFDB_CONNECTED) {
        return enif_make_badarg(env);
    }

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], ErlFDBClusterRes, &res)) {
        return enif_make_badarg(env);
    }
    c = (ErlFDBCluster*) res;

    if(!enif_inspect_binary(env, argv[1], &bin)) {
        return enif_make_badarg(env);
    }

    // FoundationDB requires a dbname that is "DB"
    if(bin.size != 2 || bin.data[0] != 'D' || bin.data[1] != 'B') {
        return enif_make_badarg(env);
    }

    future = fdb_cluster_create_database(
            c->cluster, (const uint8_t*) bin.data, bin.size);

    return erlfdb_create_future(env, future, ErlFDB_FT_DATABASE);
}


static ERL_NIF_TERM
erlfdb_database_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // ToDo: Actually write this bit
    return enif_make_badarg(env);
}


static ERL_NIF_TERM
erlfdb_database_create_transaction(
        ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlFDBSt* st = (ErlFDBSt*) enif_priv_data(env);
    ErlFDBDatabase* d;
    ErlFDBTransaction* t;
    FDBTransaction* transaction;
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

    ret = enif_make_resource(env, t);
    enif_release_resource(t);
    return T2(env, ATOM_ok, T2(env, ATOM_erlfdb_transaction, ret));
}


static ERL_NIF_TERM
erlfdb_transaction_set_option(
        ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // ToDo: Implement this
    return enif_make_badarg(env);
}


static ERL_NIF_TERM
erlfdb_transaction_set_read_version(
        ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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

    if(!enif_get_int64(env, argv[1], &erl_vsn)) {
        return enif_make_badarg(env);
    }

    fdb_vsn = erl_vsn;

    fdb_transaction_set_read_version(t->transaction, fdb_vsn);

    return ATOM_ok;
}


static ERL_NIF_TERM
erlfdb_transaction_get_read_version(
        ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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

    future = fdb_transaction_get_read_version(t->transaction);

    return erlfdb_create_future(env, future, ErlFDB_FT_VERSION);
}


static ERL_NIF_TERM
erlfdb_transaction_get(
        ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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

    if(!enif_inspect_binary(env, argv[1], &key)) {
        return enif_make_badarg(env);
    }

    if(enif_compare(argv[2], ATOM_true)) {
        snapshot = 1;
    } else if(enif_compare(argv[2], ATOM_false)) {
        snapshot = 0;
    } else {
        return enif_make_badarg(env);
    }

    future = fdb_transaction_get(
            t->transaction, (uint8_t*) key.data, key.size, snapshot);

    return erlfdb_create_future(env, future, ErlFDB_FT_VERSION);
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
    NIF_FUNC(erlfdb_future_is_ready, 1),
    NIF_FUNC(erlfdb_future_get_error, 1),
    NIF_FUNC(erlfdb_future_get, 1),

    NIF_FUNC(erlfdb_create_cluster, 1),
    NIF_FUNC(erlfdb_cluster_set_option, 3),
    NIF_FUNC(erlfdb_cluster_create_database, 2),

    NIF_FUNC(erlfdb_database_set_option, 3),
    NIF_FUNC(erlfdb_database_create_transaction, 1),

    NIF_FUNC(erlfdb_transaction_set_option, 3),
    NIF_FUNC(erlfdb_transaction_set_read_version, 2),
    NIF_FUNC(erlfdb_transaction_get_read_version, 1),
    NIF_FUNC(erlfdb_transaction_get, 3)
};
#undef NIF_FUNC


ERL_NIF_INIT(erlfdb_nif, funcs, &erlfdb_load, NULL, NULL, &erlfdb_unload);
