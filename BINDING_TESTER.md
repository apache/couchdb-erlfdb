Running the bindingstester
===

# Easy Button: devcontainer

The image build takes care of setting up an environment where the Erlang
bindings can be tested. It clones the FDB repo and patches the necessary
files to register Erlang as a known binding. Assuming erlfdb has been built
using `make`; the bindings tests be run directly via

```bash
ERL_LIBS=/usr/src/erlfdb/ /usr/src/foundationdb/bindings/bindingtester/bindingtester.py erlang
```

# Manual Approach

This assumes that all FoundationDB dependencies are installed properly. See
the FoundationDB documentation for information on the dependencies.

1. Download and build FoundationDB
---

```bash
$ git clone https://github.com/apple/foundationdb
$ cd foundationdb
$ # Optionally checkout a specific release branch
$ # git checkout -b release-6.1 origin/release-6.1
$ mkdir _build
$ cd _build
$ cmake ..
$ make -j4
```

2. Tweak `bindingtester` to be able to run our Erlang client
---

```bash
$ cd ../bindings/bindingtester
$ vi __init__.py # Comment out the sys.path injection on line 25
$ vi known_testers.py # Add the following line to the `testers` hash
```

```python
'erlang': Tester('erlang', '/Users/davisp/github/labs-cloudant/couchdb-erlfdb/test/tester.es', 2040, MAX_API_VERSION, MAX_API_VERSION, types=ALL_TYPES),
```

3. Start a temporary fdbserver instance
---

*This should be done in a second shell*

```bash
$ mkdir ~/tmp/fdbtest
$ cd ~/tmp/fdbtest
$ echo "foo:bar@127.0.0.1:4689" > fdb.cluster
$ /Users/davisp/github/davisp/foundationdb/_build/bin/fdbserver \
    -p 127.0.0.1:4689 \
    -C fdb.cluster \
    -d . \
    -L .
```

4. Configure the temporary fdbserver instance
---

*This only needs to be done once after step 3*

```bash
$ cd ~/tmp/fdbtest
$ /Users/davisp/github/davisp/foundationdb/_build/bin/fdbcli
Database created
```

5. Start the binding test
---

*Notice that the ERL_LIBS=... command is one long single line.*

```bash
$ cd /Users/davisp/github/davisp/foundationdb/bindings/bindingtester
$ ERL_LIBS=/Users/davisp/github/labs-cloudant/couchdb-erlfdb/ PYTHONPATH=/Users/davisp/github/davisp/foundationdb/_build/bindings/python/ ./bindingtester.py --cluster-file /Users/davisp/tmp/fdbtest/fdb.cluster erlang
```

# Testing Notes

By default, `bindingtester.py` runs the `scripted.py` test which is a deterministic set of tests. To really try and soak test the bindings you should add the following command line parameters:

`--test-name api --num-ops 10000`

The `api` test is a large randomly generated set of test instructions and should exercise a number of combinations of parameters exercising large portions of the bindings.

If you do encounter an error in tests you can add these options to try and debug the issue:

`--seed $SEED --bisect`

Where `$SEED` is printed in the output of the failed test run.