(ns clojurewerkz.cassaforte.ddl.cql-row
  (:import [org.apache.cassandra.thrift CqlRow]))

(defn get-key
  [^CqlRow row]
  (.getKey row))

(defn get-columns
  [^CqlRow row]
  (.getColumns row))
