(ns clojurewerkz.cassaforte.thrift.column-family-definition
  (:import [org.apache.cassandra.thrift CfDef]))

(defn get-keyspace
  [^CfDef cfdef]
  (.getKeyspace cfdef))

(defn get-name
  [^CfDef cfdef]
  (.getName cfdef))

(defn get-column-type
  [^CfDef cfdef]
  (.getColumn_type cfdef))

(defn get-comparator-type
  [^CfDef cfdef]
  (.getComparator_type cfdef))

(defn get-column-metadata
  [^CfDef cfdef]
  (.getColumn_metadata cfdef))

(def get-cdefs get-column-metadata)

;; get-comment
;; get-read-repair-chance
;; get-default-validation-class
;; get-id