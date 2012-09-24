(ns clojurewerkz.cassaforte.test-helper
  (:use     clojurewerkz.cassaforte.utils)
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.thrift.core :as thrift]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]))

(defn initialize-cql
  [f]
  (cc/connect! "127.0.0.1")
  (cql/set-keyspace! "CassaforteTest1")
  (f))

;;
;; TBD
;;
(defn initialize-thrift
  [f])