(ns clojurewerkz.cassaforte.test-helper
  (:use     clojurewerkz.cassaforte.utils)
  (:require [clojurewerkz.cassaforte.client :as thrift-client]
            [clojurewerkz.cassaforte.cql.client :as cql-client]
            [clojurewerkz.cassaforte.thrift.core :as thrift]
            [clojurewerkz.cassaforte.cql.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]))

(defn initialize-thrift
  [f]
  (when (not (bound? (var thrift-client/*cassandra-client*)))
    (thrift-client/connect! "127.0.0.1")
    (with-thrift-exception-handling
      (cql/set-keyspace "cassaforte_test_1")))
  (f))

(defn initialize-cql
  [f]
  (when (not (bound? (var cql-client/*client*)))
    (cql-client/connect! "127.0.0.1")
    (with-thrift-exception-handling
      (cql/set-keyspace "cassaforte_test_1")))
  (f))

;;
;; TBD
;;
(defn initialize-thrift
  [f])
