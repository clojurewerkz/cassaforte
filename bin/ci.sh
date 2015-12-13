make start_one_node_cluster                                 && \
lein2 all do clean, test, clean                             && \
make stop_cluster                                           && \
make start_one_node_cluster CASSANDRA_VERSION=binary:2.0.13 && \
lein2 do test, clean                                        && \
make stop_cluster                                           && \
make start_one_node_cluster CASSANDRA_VERSION=binary:3.0.1  && \
lein2 do test, clean                                        && \
make stop_cluster
