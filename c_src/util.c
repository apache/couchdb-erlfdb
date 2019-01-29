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

#include <string.h>

#include "atoms.h"
#include "util.h"


ERL_NIF_TERM
erlfdb_erlang_error(ErlNifEnv* env, fdb_error_t err)
{
    const char* msg = fdb_get_error(err);
    unsigned char* bin;
    ERL_NIF_TERM ret;

    bin = enif_make_new_binary(env, strlen(msg), &ret);
    memcpy(bin, msg, strlen(msg));

    return T2(env, ATOM_error, ret);
}