(ns clojurewerkz.cassaforte.conversion
  (:use [clojure.walk :only [stringify-keys]]
        [clojurewerkz.support.string :only [to-byte-buffer]])
  (:import [org.apache.cassandra.thrift ConsistencyLevel KsDef CfDef CqlPreparedResult CqlResult CqlRow Column]
           java.util.List
           java.nio.ByteBuffer))



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


(defn from-cql-prepared-result
  [^CqlPreparedResult result]
  (let [id (.getItemId result)]
    {:id id
     :item-id id
     :count (.getCount result)
     :variable-names (.getVariable_names result)
     :variable-types (.getVariable_types result)}))

(defn from-cql-column
  [^Column column]
  {:name      (String. ^bytes (.getName column) "UTF-8")
   :value     (.getValue column)
   :ttl       (.getTtl column)
   :timestamp (.getTimestamp column)})

(defn from-cql-row
  [^CqlRow row]
  {:key (String. ^bytes (.getKey row) "UTF-8")
   :columns (map from-cql-column (.getColumns row))})

(defn from-cql-result
  [^CqlResult result]
  {:num    (.getNum result)
   :schema (.getSchema result)
   :type   (.getType result)
   :rows   (map from-cql-row (.getRows result))})