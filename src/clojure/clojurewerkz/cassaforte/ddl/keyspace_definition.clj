(ns clojurewerkz.cassaforte.ddl.keyspace-definition
  (:use [clojure.walk :only [stringify-keys]])
  (:import [org.apache.cassandra.thrift KsDef]
           java.util.List))


;;
;; Getters
;;

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


;;
;; Builders
;;

(defn ^org.apache.cassandra.thrift.KsDef build-keyspace-definition
  ([^String name ^String strategy-class ^List column-family-defs]
     (KsDef. name strategy-class column-family-defs))
  ([^String name ^String strategy-class ^List column-family-defs & {:keys [strategy-opts]}]
     (let [ks-def (KsDef. name strategy-class column-family-defs)]
       (when strategy-opts
         (.setStrategy_options ks-def (stringify-keys strategy-opts)))
       (when (not (empty? column-family-defs))
         (.setCf_defs ks-def column-family-defs))
       ks-def)))

(def build-kd build-keyspace-definition)