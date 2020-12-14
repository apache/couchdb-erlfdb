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

#ifndef ERLFDB_RESOURCES_H
#define ERLFDB_RESOURCES_H

#include <stdbool.h>

#include "erl_nif.h"
#include "fdb.h"


extern ErlNifResourceType* ErlFDBFutureRes;
extern ErlNifResourceType* ErlFDBDatabaseRes;
extern ErlNifResourceType* ErlFDBTransactionRes;


typedef enum _ErlFDBFutureType
{
    ErlFDB_FT_NONE = 0,
    ErlFDB_FT_VOID,
    ErlFDB_FT_INT64,
    ErlFDB_FT_KEY,
    ErlFDB_FT_VALUE,
    ErlFDB_FT_STRING_ARRAY,
    ErlFDB_FT_KEYVALUE_ARRAY
} ErlFDBFutureType;


typedef struct _ErlFDBFuture
{
    FDBFuture* future;
    ErlFDBFutureType ftype;
    ErlNifPid pid;
    ErlNifEnv* pid_env;
    ErlNifEnv* msg_env;
    ERL_NIF_TERM msg_ref;
    ErlNifMutex* lock;
    bool cancelled;
} ErlFDBFuture;


typedef struct _ErlFDBDatabase
{
    FDBDatabase* database;
} ErlFDBDatabase;


typedef struct _ErlFDBTransaction
{
    FDBTransaction* transaction;
    ERL_NIF_TERM owner;
    unsigned int txid;
    bool read_only;
    bool writes_allowed;
    bool has_watches;
} ErlFDBTransaction;


int erlfdb_init_resources(ErlNifEnv* env);
void erlfdb_future_dtor(ErlNifEnv* env, void* obj);
void erlfdb_database_dtor(ErlNifEnv* env, void* obj);
void erlfdb_transaction_dtor(ErlNifEnv* env, void* obj);


int erlfdb_transaction_is_owner(ErlNifEnv* env, ErlFDBTransaction* t);


#endif // Included resources.h
