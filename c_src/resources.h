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

#include "erl_nif.h"
#include "fdb.h"


extern ErlNifResourceType* ErlFDBFutureRes;
extern ErlNifResourceType* ErlFDBClusterRes;
extern ErlNifResourceType* ErlFDBDatabaseRes;
extern ErlNifResourceType* ErlFDBTransactionRes;

typedef struct _ErlFDBFuture
{
    FDBFuture* future;
} ErlFDBFuture;


typedef struct _ErlFDBCluster
{
    FDBCluster* cluster;
} ErlFDBCluster;


typedef struct _ErlFDBDatabase
{
    FDBDatabase* db;
} ErlFDBDatabase;


typedef struct _ErlFDBTransaction
{
    FDBTransaction* tx;
} ErlFDBTransaction;


int erlfdb_init_resources(ErlNifEnv* env);
void erlfdb_future_dtor(ErlNifEnv* env, void* obj);
void erlfdb_cluster_dtor(ErlNifEnv* env, void* obj);
void erlfdb_database_dtor(ErlNifEnv* env, void* obj);
void erlfdb_transaction_dtor(ErlNifEnv* env, void* obj);


#endif // Included resources.h