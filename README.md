An Erlang Binding to FoundationDB
===

[![CI](https://github.com/apache/couchdb-erlfdb/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/couchdb-erlfdb/actions/workflows/ci.yml)[![Coverage](https://coveralls.io/repos/github/apache/couchdb-erlfdb/badge.svg?branch=main)](https://coveralls.io/github/apache/couchdb-erlfdb?branch=main)

This project is a NIF wrapper for the FoundationDB C API. Documentation on
the main API can be found [here][fdb_docs].

This project also provides a conforming implementation of the [Tuple] and
[Directory] layers.

[fdb_docs]: https://apple.github.io/foundationdb/api-c.html
[Tuple]: https://github.com/apple/foundationdb/blob/master/design/tuple.md
[Directory]: https://apple.github.io/foundationdb/developer-guide.html#directories


Building
---

Assuming you have installed the FoundationDB C API library, building erlfdb
is as simple as:

    $ make

Alternatively, adding erlfdb as a rebar dependency should Just Work ®.


Documentation for installing FoundationDB can be found [here for macOS]
or [here for Linux].

[here for macOS]: https://apple.github.io/foundationdb/getting-started-mac.html
[here for Linux]: https://apple.github.io/foundationdb/getting-started-linux.html


Quick Example
---

A simple example showing how to open a database and read and write keys:

```erlang

Eshell V9.3.3.6  (abort with ^G)
1> Db = erlfdb:open(<<"/usr/local/etc/foundationdb/fdb.cluster">>).
{erlfdb_database,#Ref<0.2859661758.3941466120.85406>}
2> ok = erlfdb:set(Db, <<"foo">>, <<"bar">>).
ok
3> erlfdb:get(Db, <<"foo">>).
<<"bar">>
4> erlfdb:get(Db, <<"bar">>).
not_found
```

Binding Tester
---

FoundationDB has a custom binding tester that can be used to test whether
changes have broken compatibility. See the [BINDING_TESTER](BINDING_TESTER.md)
documentation for instructions on building and running that system.