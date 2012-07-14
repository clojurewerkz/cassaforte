(ns clojurewerkz.cassaforte.conversion
  (:require [clojurewerkz.cassaforte.bytes :as cb])
  (:use [clojure.walk :only [stringify-keys]]
        [clojurewerkz.support.string :only [to-byte-buffer from-byte-buffer]])
  (:import [org.apache.cassandra.thrift ConsistencyLevel KsDef CfDef CqlPreparedResult CqlResult CqlRow Column CqlMetadata]
           java.util.List
           java.nio.ByteBuffer))

;;
;; Implementation
;;

(defn from-cql-column
  [^Column column]
  {:name      (.getName column)
   :value     (.getValue column)
   :ttl       (.getTtl column)
   :timestamp (.getTimestamp column)})

(defn from-cql-row
  [^CqlRow row]
  {:key     (.getKey row)
   :columns (doall (map from-cql-column (.getColumns row)))})

(defn from-cql-types
  "Transforms a map of CQL column names/types with byte buffer keys into
   an immutable Clojure map with string keys"
  [^java.util.Map m]
  (reduce (fn [acc ^java.util.Map$Entry entry]
            (assoc acc (.getKey entry) (.getValue entry)))
          {}
          m))

(defn- deserialize-row
  "Returns a row with all column values deserialized from byte arrays to strings, numerics, et cetera
   according to the schema information"
  [{:keys [columns] :as row} {:keys [value-types default-value-type name-types default-name-type] :as schema}]
  (let [cols (for [col columns
                   :let [^bytes k  (:name col)
                         k-buf     (ByteBuffer/wrap k)
                         name-type (get name-types k-buf default-name-type)
                         val-type  (get value-types k-buf default-value-type)
                         val       (:value col)]]
               (assoc col :name  (when k
                                   (cb/deserialize name-type k))
                      :value (when val
                               (cb/deserialize val-type val))))]
    (assoc row :columns cols)))

(defn- deserialize-rows
  [rows schema]
  (if schema
    (map (fn [row]
           (deserialize-row row schema)) rows)
    rows))

(defn from-cql-metadata
  [^CqlMetadata md]
  {:value-types        (from-cql-types (.getValue_types md))
   :name-types         (from-cql-types  (.getName_types md))
   :default-value-type (.getDefault_value_type md)
   :default-name-type  (.getDefault_name_type md)})


;;
;; API
;;

(defn from-cql-result
  [^CqlResult result]
  (let [raw-schema (.getSchema result)
        schema     (when raw-schema
                     (from-cql-metadata raw-schema))
        base   {:num    (.getNum result)
                :type   (.getType result)
                :rows   (deserialize-rows
                         (map from-cql-row (.getRows result))
                         schema)}]
    (if raw-schema
      (assoc base :schema schema)
      base)))

(defn from-cql-prepared-result
  [^CqlPreparedResult result]
  (let [id (.getItemId result)]
    {:id id
     :item-id id
     :count (.getCount result)
     :variable-names (.getVariable_names result)
     :variable-types (.getVariable_types result)}))




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
