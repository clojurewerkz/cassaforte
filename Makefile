CCM_DIR           := ./ccm
CONFIG_DIR        := /tmp/cassaforte-data
CLUSTER_NAME      := cassaforte_cluster
CASSANDRA_VERSION := binary:2.1.3

maybe_install_ccm:
	test -s ccm || pip install ccm

prepare_tmp_dir:
	rm -fr $(CONFIG_DIR) ;\
	mkdir -p $(CONFIG_DIR)

prepare_aliases:
	sudo ifconfig lo0 alias 127.0.0.2 up ;\
	sudo ifconfig lo0 alias 127.0.0.2 up

start_one_node_cluster: maybe_install_ccm prepare_tmp_dir
	ccm create $(CLUSTER_NAME) -n 1 -s -i 127.0.0. -b -v $(CASSANDRA_VERSION) --config-dir=$(CONFIG_DIR)

start_three_node_cluster: maybe_install_ccm prepare_tmp_dir
	ccm create $(CLUSTER_NAME) -n 3 -s -i 127.0.0. -b -v $(CASSANDRA_VERSION) --config-dir=$(CONFIG_DIR)

.PHONY: clean
stop_cluster:
	ps ax | grep java | grep org.apache.cassandra.service.CassandraDaemon | grep -v grep | awk '{print $$1}' | xargs kill -9

.PHONY: clean
clean:
	rm -fr $(CCM_DIR)
