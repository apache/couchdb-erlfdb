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
    ERL_NIF_TERM code = enif_make_int(env, err);
    ERL_NIF_TERM tuple = T2(env, ATOM_erlfdb_error, code);
    return enif_raise_exception(env, tuple);
}


int
erlfdb_get_boolean(ERL_NIF_TERM term, fdb_bool_t* ret)
{
    if(IS_ATOM(term, true) == 0) {
        *ret = 1;
    } else if(IS_ATOM(term, false) == 0) {
        *ret = 0;
    } else {
        return 0;
    }

    return 1;
}


int
erlfdb_get_key_selector(
        ErlNifEnv* env,
        ERL_NIF_TERM selector,
        ErlNifBinary* bin,
        fdb_bool_t* or_equal,
        int* offset
    )
{
    const ERL_NIF_TERM* tuple;
    int arity;

    if(!enif_get_tuple(env, selector, &arity, &tuple)) {
        return 0;
    }

    if(arity != 2 && arity != 3) {
        return 0;
    }

    if(!enif_inspect_binary(env, tuple[0], bin)) {
        return 0;
    }

    if(arity == 2) {
        if(IS_ATOM(tuple[1], lt) == 0) {
            *or_equal = 0;
            *offset = 0;
        } else if(IS_ATOM(tuple[1], lteq) == 0) {
            *or_equal = 1;
            *offset = 0;
        } else if(IS_ATOM(tuple[1], gt) == 0) {
            *or_equal = 1;
            *offset = 1;
        } else if(IS_ATOM(tuple[1], gteq) == 0) {
            *or_equal = 0;
            *offset = 1;
        } else {
            return 0;
        }
    } else if(arity == 3) {
        if(IS_ATOM(tuple[1], true) == 0) {
            *or_equal = 1;
        } else if(IS_ATOM(tuple[1], false) == 0) {
            *or_equal = 0;
        } else {
            return 0;
        }

        if(!enif_get_int(env, tuple[2], offset)) {
            return 0;
        }
    } else {
        // Technically this is dead code, but keeping
        // it here in case the arity conditional earlier
        // in this function is ever changed.
        return 0;
    }

    return 1;
}