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

#include "resources.h"


ErlNifResourceType* ErlFDBFutureRes;
ErlNifResourceType* ErlFDBDatabaseRes;
ErlNifResourceType* ErlFDBTransactionRes;


int
erlfdb_init_resources(ErlNifEnv* env)
{

    ErlFDBFutureRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_future",
            erlfdb_future_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBFutureRes == NULL) {
        return 0;
    }

    ErlFDBDatabaseRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_database",
            erlfdb_database_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBDatabaseRes == NULL) {
        return 0;
    }

    ErlFDBTransactionRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_transaction",
            erlfdb_transaction_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBTransactionRes == NULL) {
        return 0;
    }


    return 1;
}

void
erlfdb_future_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBFuture* f = (ErlFDBFuture*) obj;

    if(f->future != NULL) {
        fdb_future_destroy(f->future);
    }

    if(f->msg_env != NULL) {
        enif_free_env(f->msg_env);
    }

    if(f->lock != NULL) {
        enif_mutex_destroy(f->lock);
    }
}


void
erlfdb_database_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBDatabase* d = (ErlFDBDatabase*) obj;

    if(d->database != NULL) {
        fdb_database_destroy(d->database);
    }
}


void
erlfdb_transaction_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBTransaction* t = (ErlFDBTransaction*) obj;

    if(t->transaction != NULL) {
        fdb_transaction_destroy(t->transaction);
    }
}


int
erlfdb_transaction_is_owner(ErlNifEnv* env, ErlFDBTransaction* t)
{
    ErlNifPid pid;
    ERL_NIF_TERM self;

    enif_self(env, &pid);
    self = enif_make_pid(env, &pid);

    return enif_compare(t->owner, self) == 0;
}
