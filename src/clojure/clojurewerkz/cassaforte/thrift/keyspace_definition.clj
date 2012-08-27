(ns clojurewerkz.cassaforte.thrift.keyspace-definition
  (:import [org.apache.cassandra.thrift KsDef]))

(defn get-name
  [^KsDef ks-def]
  (.getName ks-def))

(defn get-strategy-class
  "Rerturns strategy class of current keyspace definition"
  [^KsDef ks-def]
  (.getStrategy_class ks-def))

(defn is-durable-writes?
  [^KsDef ks-def]
  (.isDurable_writes ks-def))

(defn get-strategy-options
  [^KsDef ks-def]
  (.getStrategy_options ks-def))

(defn get-column-family-definitions
  [^KsDef ks-def]
  (.getCf_defs ks-def))

(def get-cf-defs get-column-family-definitions)