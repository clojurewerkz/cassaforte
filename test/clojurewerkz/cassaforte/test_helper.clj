(ns clojurewerkz.cassaforte.test-helper
  (:use     clojurewerkz.cassaforte.utils)
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.thrift.core :as thrift]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]))

(defn initialize-cql
  [f]
  (cc/connect! "127.0.0.1")
  (with-thrift-exception-handling
    (cql/execute "DROP KEYSPACE CassaforteTest1")
    (cql/execute "CREATE KEYSPACE CassaforteTest1 WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;")
    (cql/set-keyspace! "CassaforteTest1"))
  (f))

;;
;; TBD
;;
(defn initialize-thrift
  [f])