(ns clojurewerkz.cassaforte.conversion
  (:use [clojure.walk :only [stringify-keys]])
  (:import [org.apache.cassandra.thrift ConsistencyLevel KsDef CfDef]
           java.util.List))



;;
;; API
;;

(defprotocol ConsistencyLevelConversion
  (^org.apache.cassandra.thrift.ConsistencyLevel to-consistency-level [input] "Converts the input to one of the ConsistencyLevel enum values"))

(extend-protocol ConsistencyLevelConversion
  ConsistencyLevel
  (to-consistency-level [^ConsistencyLevel input]
    input)

  String
  (to-consistency-level [^String input]
    (ConsistencyLevel/valueOf (.toUpperCase input)))

  clojure.lang.Keyword
  (to-consistency-level [^clojure.lang.Keyword input]
    (ConsistencyLevel/valueOf (.toUpperCase (name input)))))



(defn ^org.apache.cassandra.thrift.KsDef to-keyspace-definition
  ([^String name ^String strategy-class ^List column-family-defs]
     (KsDef. name strategy-class column-family-defs))
  ([^String name ^String strategy-class ^List column-family-defs & {:keys [strategy-opts]}]
     (let [ks-def (KsDef. name strategy-class column-family-defs)]
       (when strategy-opts
         (.setStrategy_options ks-def (stringify-keys strategy-opts)))
       ks-def)))


(defn ^org.apache.cassandra.thrift.CfDef to-column-family-definition
  ([^String keyspace ^String name]
     (CfDef. keyspace name))
  ([^String keyspace ^String name & {:keys [column-type comparator-type]
                                     :or {column-type "Standard"
                                          comparator-type "org.apache.cassandra.db.marshal.BytesType"}}]
     (let [cf-def (CfDef. keyspace name)]
       ;; TODO
       cf-def)))
