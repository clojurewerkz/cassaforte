make start_one_node_cluster CASSANDRA_VERSION=binary:$CASSANDRA_VERSION && \
lein2 all do clean, test, clean                                         && \
make stop_cluster                                                       && \
