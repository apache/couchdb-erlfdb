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


#ifndef ERLFDB_ATOMS_H
#define ERLFDB_ATOMS_H

#include "erl_nif.h"


#define ATOM_MAP(NAME) extern ERL_NIF_TERM ATOM_##NAME;
#include "atom_names.h"
#undef ATOM_MAP


void erlfdb_init_atoms();


#endif // Included atoms.h