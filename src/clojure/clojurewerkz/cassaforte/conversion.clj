(ns clojurewerkz.cassaforte.conversion
  (:require [clojurewerkz.cassaforte.bytes :as cb]
            [clojurewerkz.cassaforte.thrift.keyspace-definition :as kd]
            [clojurewerkz.cassaforte.thrift.column-family-definition :as cfd]
            [clojurewerkz.cassaforte.thrift.column-definition :as cd]
            [clojurewerkz.cassaforte.thrift.column :as col]
            [clojurewerkz.cassaforte.thrift.cql-row :as cql-row]
            [clojurewerkz.cassaforte.thrift.super-column :as scol]
            [clojurewerkz.cassaforte.thrift.column-or-super-column :as cosc])
  (:use [clojure.walk :only [stringify-keys keywordize-keys]]
        [clojurewerkz.support.string :only [to-byte-buffer from-byte-buffer]])
  (:import [org.apache.cassandra.thrift ConsistencyLevel KsDef CfDef ColumnDef CqlPreparedResult
                                        CqlResult CqlRow Column SuperColumn ColumnOrSuperColumn
                                        CqlMetadata Mutation SliceRange ColumnParent SlicePredicate ColumnPath]
           [org.apache.cassandra.utils ByteBufferUtil]
           java.nio.ByteBuffer))

;;
;; Map protocols
;;

(defprotocol DefinitionToMap
  (to-map [input] "Converts any definition to map"))

(defprotocol ToPlainHash
  (to-plain-hash
    [input]
    [input key-format]
    ""))

;;
;; Implementation
;;

(defn from-cql-types
  "Transforms a map of CQL column names/types with byte buffer keys into
   an immutable Clojure map with string keys"
  [^java.util.Map m]
  (reduce (fn [acc ^java.util.Map$Entry entry]
            (assoc acc (keyword (ByteBufferUtil/string (.getKey entry))) (.getValue entry)))
          {}
          m))

(defn deserialize-columns
  "Returns a row with all column values deserialized from byte arrays to strings, numerics, et cetera
   according to the schema information"
  [columns {:keys [value-types default-value-type name-types default-name-type] :as schema}]
  (for [col columns
                   :let [^bytes k  (:name col)
                         k-buf     (keyword (String. k "UTF-8"))
                         name-type (get name-types k-buf default-name-type)
                         val-type  (get value-types k-buf default-value-type)
                         val       (:value col)]]
               (assoc col
                 :name  (when k
                          (cb/deserialize name-type k))
                 :value (when val
                          (cb/deserialize val-type val)))))

(defn deserialize-rows
  [rows schema]
  (if schema
    (map (fn [row]
           (let [columns (deserialize-columns (:columns row) schema)]
             (assoc row :columns columns)))
         rows)
    rows))

(defn deserialize-thrift-response
  ([columns]
     (deserialize-thrift-response {:default-value-type "UTF8Type" :default-name-type "UTF8Type"}))
  ([columns schema]
     (let [deserialize-cosc (fn [x]
                              (if (cosc/is-super-column? (first columns))
                                 (deserialize-rows x schema)
                                 (deserialize-columns x schema)))]
        (-> columns
            to-map
            deserialize-cosc
            to-plain-hash))))

;;
;; API
;;

(defn from-cql-prepared-result
  [^CqlPreparedResult result]
  (let [id (.getItemId result)]
    {:id id
     :item-id id
     :count (.getCount result)
     :variable-names (.getVariable_names result)
     :variable-types (.getVariable_types result)}))

;;
;; Consistency Conversions
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

;;
;; Map conversions
;;


(extend-protocol DefinitionToMap
  java.util.HashMap
  (to-map [^clojure.lang.IPersistentMap input & opts]
    (into {} (keywordize-keys input)))

  KsDef
  (to-map [^KsDef ks-def]
    {:name (kd/get-name ks-def)
     :strategy-class (kd/get-strategy-class ks-def)
     :strategy-opts (to-map (kd/get-strategy-options ks-def))
     :cf-defs (map to-map (kd/get-cf-defs ks-def))})

  CfDef
  (to-map [^CfDef cf-def]
    {:keyspace (cfd/get-keyspace cf-def)
     :name     (cfd/get-name cf-def)
     :column-type (cfd/get-column-type cf-def)
     :comparator-type (cfd/get-comparator-type cf-def)
     :column-metadata (map to-map (cfd/get-column-metadata cf-def))})

  ColumnDef
  (to-map [^ColumnDef cdef]
    {:name (cd/get-name cdef)
     :validation-class (cd/get-validation-class cdef)})

  Column
  (to-map [^Column column]
    {:name (col/get-name column)
     :value  (col/get-value column)
     :ttl (col/get-ttl column)
     :timestamp (col/get-timestamp column)})

  CqlRow
  (to-map [^CqlRow row]
    {:key     (cql-row/get-key row)
     :columns (doall (map to-map (cql-row/get-columns row)))})

  SuperColumn
  (to-map [^SuperColumn scolumn]
    {:name (scol/get-name scolumn)
     :columns (map to-map (scol/get-columns scolumn))})

  ColumnOrSuperColumn
  (to-map [^ColumnOrSuperColumn column-or-scolumn]
    (to-map
     (if (cosc/is-super-column? column-or-scolumn)
       (cosc/get-super-column column-or-scolumn)
       (cosc/get-column column-or-scolumn))))

  CqlMetadata
  (to-map [^CqlMetadata md]
    {:value-types        (from-cql-types (.getValue_types md))
     :name-types         (from-cql-types  (.getName_types md))
     :default-value-type (.getDefault_value_type md)
     :default-name-type  (.getDefault_name_type md)})

  CqlResult
  (to-map [^CqlResult result]
    (let [raw-schema (.getSchema result)
          schema     (when raw-schema
                       (to-map raw-schema))
          base   {:num    (.getNum result)
                  :type   (.getType result)
                  :rows   (deserialize-rows
                           (map to-map (.getRows result))
                           schema)}]
      (if raw-schema
        (assoc base :schema schema)
        base)))

  java.util.List
  (to-map [list]
    (map to-map list))

  nil
  (to-map [_]
    nil)

  Object
  (to-map [obj]
    obj))





(extend-protocol ToPlainHash
  java.util.List
  (to-plain-hash
    ([cosc-map]
       (to-plain-hash cosc-map "UTF8Type"))
    ([cosc-map key-format]
       (let [list  (map to-map cosc-map)
             names (map #(or (keyword (:name %))
                             (cb/deserialize key-format (:key %)))
                        list)
             values (map #(to-plain-hash (or (:columns %) (:value %))) list)]
         (apply array-map (interleave names values)))))

  Object
  (to-plain-hash
    ([obj]
       obj)
    ([obj key-format]
       obj))

  nil
  (to-plain-hash
    ([_]
       nil)
    ([_ key-format]
       nil)))