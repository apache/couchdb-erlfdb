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

#ifndef ERLFDB_UTIL_H
#define ERLFDB_UTIL_H

#include "erl_nif.h"
#include "fdb.h"

#define ERLFDB_MAX_ATOM_LENGTH 255

#define T2(e, a, b) enif_make_tuple2(e, a, b)
#define T3(e, a, b, c) enif_make_tuple3(e, a, b, c)
#define T4(e, a, b, c, d) enif_make_tuple4(e, a, b, c, d)

ERL_NIF_TERM erlfdb_erlang_error(ErlNifEnv* env, fdb_error_t err);


#endif // Included util.h
