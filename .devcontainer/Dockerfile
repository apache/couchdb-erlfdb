ARG FDB_VERSION
ARG ERLANG_VERSION

# Grab fdbcli and client library from same image as server
FROM foundationdb/foundationdb:${FDB_VERSION} as fdb

# Debian image with Erlang installed (we need elixir for test suite)
FROM erlang:${ERLANG_VERSION}

# The FROM directive above sweeps out the ARGs so we need to re-declare here
# in order to use it again to download the FDB client package
ARG FDB_VERSION

# Install the FDB client used underneath erlfdb
RUN set -ex; \
    wget https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers/foundationdb-clients_${FDB_VERSION}-1_amd64.deb; \
    mkdir /var/lib/foundationdb; \
    dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb

# FDB bindings tester uses the Python bindings; install them from a
# package to avoid building FDB from source
# TODO FDB 6.3+ uses python3, we'll need to update it here
RUN set -ex; \
    wget https://www.foundationdb.org/downloads/${FDB_VERSION}/bindings/python/foundationdb-${FDB_VERSION}.tar.gz; \
    tar zxf foundationdb-${FDB_VERSION}.tar.gz; \
    cd foundationdb-${FDB_VERSION}; \
    python setup.py install

# Clone FoundationDB repo to retrieve bindings tester package and
# patch it to support erlfdb
COPY add_erlang_bindings.patch /tmp/
RUN set -ex; \
    git clone --branch ${FDB_VERSION} --single-branch https://github.com/apple/foundationdb /usr/src/foundationdb; \
    cd /usr/src/foundationdb; \
    git apply /tmp/add_erlang_bindings.patch

# `dig` is used by the script that creates the FDB cluster file
RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        dnsutils

COPY --from=fdb /var/fdb/scripts/create_cluster_file.bash /usr/local/bin/

CMD sleep infinity
