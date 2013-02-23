(ns clojurewerkz.cassaforte.test-helper
  (:use     clojurewerkz.cassaforte.utils)
  (:require [clojurewerkz.cassaforte.thrift.client :as thrift-client]
            [clojurewerkz.cassaforte.thrift.schema :as thrift-schema]
            [clojurewerkz.cassaforte.cql.client :as cql-client]
            [clojurewerkz.cassaforte.cluster.client :as cluster-client]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.thrift.core :as thrift]))

(defn initialize-thrift
  [f]
  (when (not (bound? (var thrift-client/*cassandra-client*)))
    (thrift-client/connect! "127.0.0.1")
    (with-thrift-exception-handling
      (thrift-schema/set-keyspace "cassaforte_test_1")))
  (f))

(defn initialize-cql
  [f]
  (when (not (bound? (var cql-client/*client*)))
    (cql-client/connect! "127.0.0.1")
    (with-native-exception-handling
      (cql/use-keyspace "cassaforte_test_1")))
  (f))

(defn initialize-cluster
  [f]
  (when (not (bound? (var cluster-client/*client*)))
    (cluster-client/connect! ["192.168.60.10" "192.168.60.11" "192.168.60.12"]))
  (f))
