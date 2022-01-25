ARG FDB_VERSION
ARG ERLANG_VERSION

# Grab fdbcli and client library from same image as server
FROM foundationdb/foundationdb:${FDB_VERSION} as fdb

# Debian image with Erlang installed
FROM erlang:${ERLANG_VERSION}

# The FROM directive above sweeps out the ARGs so we need to re-declare here
# in order to use it again to download the FDB client package
ARG FDB_VERSION

# Install the FDB client used underneath erlfdb
RUN set -ex; \
    wget https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb; \
    mkdir /var/lib/foundationdb; \
    dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb; \
    rm foundationdb-clients_${FDB_VERSION}-1_amd64.deb


# Clone FoundationDB repo to retrieve bindings tester package and
# patch it to support erlfdb
COPY add_erlang_bindings.patch /tmp/
RUN set -ex; \
    git clone --branch ${FDB_VERSION} --depth 1 https://github.com/apple/foundationdb /usr/src/foundationdb; \
    cd /usr/src/foundationdb; \
    git apply /tmp/add_erlang_bindings.patch

# `dig` is used by the script that creates the FDB cluster file
RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        dnsutils \
        python3-setuptools \
        python3-pip; \
    rm -rf /var/lib/apt/lists/*

# FDB bindings tester uses the Python bindings
RUN pip3 install foundationdb==${FDB_VERSION}

COPY create_cluster_file.bash /usr/local/bin/

CMD sleep infinity
