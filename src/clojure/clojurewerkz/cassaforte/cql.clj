(ns clojurewerkz.cassaforte.cql
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:import clojurewerkz.cassaforte.CassandraClient
           [org.apache.cassandra.thrift Compression CqlResult CqlRow CqlResultType]))


;;
;; API
;;

(defn ^org.apache.cassandra.thrift.CqlResult
  execute-raw
  "Executes a CQL query given as a string. No argument replacement (a la JDBC) is performed."
  ([^String query]
     (.executeCqlQuery ^CassandraClient cc/*cassandra-client* query))
  ([^String query ^Compression compression]
     (.executeCqlQuery ^CassandraClient cc/*cassandra-client* query compression)))


(defn void-result?
  "Returns true if the provided CQL query result is of type void (carries no result set)"
  [^CqlResult result]
  (= (.getType result) CqlResultType/VOID))

(defn int-result?
  "Returns true if the provided CQL query result is of type int (carries a single numerical value)"
  [^CqlResult result]
  (= (.getType result) CqlResultType/INT))

(defn rows-result?
  "Returns true if the provided CQL query result carries a result set"
  [^CqlResult result]
  (= (.getType result) CqlResultType/ROWS))