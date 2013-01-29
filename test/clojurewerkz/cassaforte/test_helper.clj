(ns clojurewerkz.cassaforte.test-helper
  (:use     clojurewerkz.cassaforte.utils)
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.thrift.core :as thrift]
            [clojurewerkz.cassaforte.cql.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]))

(defn initialize-cql
  [f]
  (when (not (bound? (var cc/*cassandra-client*)))
    (cc/connect! "127.0.0.1")
    (with-thrift-exception-handling
      (cql/set-keyspace "cassaforte_test_1")))
  (f))

;;
;; TBD
;;
(defn initialize-thrift
  [f])
