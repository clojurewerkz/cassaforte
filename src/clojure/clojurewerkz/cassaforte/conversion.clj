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
           java.util.List
           java.nio.ByteBuffer))

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

(defn deserialize-row
  "Returns a row with all column values deserialized from byte arrays to strings, numerics, et cetera
   according to the schema information"
  [{:keys [columns] :as row} {:keys [value-types default-value-type name-types default-name-type] :as schema}]
  (let [cols (for [col columns
                   :let [^bytes k  (:name col)
                         k-buf     (keyword (String. k "UTF-8"))
                         name-type (get name-types k-buf default-name-type)
                         val-type  (get value-types k-buf default-value-type)
                         val       (:value col)]]
               (assoc col
                 :name  (when k
                          (cb/deserialize name-type k))
                 :value (when val
                          (cb/deserialize val-type val))))]
    (assoc row :columns cols)))

(defn deserialize-rows
  [rows schema]
  (if schema
    (map (fn [row]
           (deserialize-row row schema)) rows)
    rows))

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
;; Value encoders
;;

(defprotocol ValueEncoder
  (encode [value] "Encodes the value"))

(extend-protocol ValueEncoder
  java.lang.Long
  (encode [v]
    (ByteBufferUtil/bytes #^long v))

  java.lang.Integer
  (encode [v]
    (ByteBufferUtil/bytes ^int v))

  java.lang.String
  (encode [v]
    (ByteBuffer/wrap (.getBytes v "UTF-8")))

  )

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

(defn ^ColumnDef build-column-definition
  [^String name ^String validation-class]
  (ColumnDef. (to-byte-buffer name) validation-class))

(def build-cd build-column-definition)

(defn ^org.apache.cassandra.thrift.CfDef build-column-family-definition
  ([^String keyspace ^String name]
     (CfDef. keyspace name))
  ([^String keyspace ^String name ^List cdefs & {:keys [column-type comparator-type]
                                                 :or {column-type "Standard"
                                                      comparator-type "org.apache.cassandra.db.marshal.BytesType"}}]
     (let [cfdef (build-column-family-definition keyspace name)]
       (.setColumn_type cfdef column-type)
       (.setComparator_type cfdef comparator-type)
       (doseq [cd cdefs]
         (.addToColumn_metadata cfdef cd))
       cfdef)))

(def build-cfd build-column-family-definition)

(defprotocol
    CoscConversion
  (^ColumnOrSuperColumn build-cosc [input] "Converts given instance to ColumnOrSupercolumn"))

(extend-protocol CoscConversion
  Column
  (build-cosc [^Column input]
    (.setColumn (ColumnOrSuperColumn.) input))
  SuperColumn
  (build-cosc [^SuperColumn input]
    (.setSuper_column (ColumnOrSuperColumn.) input)))

(defn ^Column build-column
  "Converts clojure map to column"
  ([^String key ^String value]
     (build-column to-byte-buffer key value (System/currentTimeMillis)))
  ([^clojure.lang.IFn encoder ^String key ^String value ^Long timestamp]
      (-> (Column.)
          (.setName (encoder (name key)))
          (.setValue (encode value))
          (.setTimestamp timestamp))))

(defn build-super-column
  "Convert a clojure map to supercolumn"
  ([^String key ^clojure.lang.IPersistentMap column-map]
     (build-super-column to-byte-buffer key column-map (System/currentTimeMillis)))
  ([^clojure.lang.IFn encoder ^String key ^clojure.lang.IPersistentMap column-map ^Long timestamp]
     (let [columns (map (fn [[key value]] (build-column encoder key value timestamp)) column-map)]
       (-> (SuperColumn.)
           (.setName (encoder key))
           (.setColumns (java.util.ArrayList. columns))))))

(defn build-mutation
  [cosc]
  (.setColumn_or_supercolumn (Mutation.) (build-cosc cosc)))

(defn build-slice-range
  "A SliceRange is a structure that stores basic range, ordering and limit information for a query
   that will return multiple columns. It could be thought of as Cassandra's version of LIMIT and ORDER BY.

   Params:
     :start (binary) - The column name to start the slice with. This attribute is not required, though
                       there is no default value, and can be safely set to '', i.e., an empty byte array, to
                       start with the first column name. Otherwise, it must be a valid value under the rules
                       of the Comparator defined for the given ColumnFamily.

    :finish (binary) - The column name to stop the slice at. This attribute is not required,
                       though there is no default value, and can be safely set to an empty byte array to not
                       stop until count results are seen. Otherwise, it must also be a valid value to the
                       ColumnFamily Comparator.

    :reversed (bool) - Whether the results should be ordered in reversed order. Similar to
                      ORDER BY blah DESC in SQL. When reversed is true, start will determine the right end
                      of the range while finish will determine the left, meaning start must be >= finish.

    :count (integer), default is 100 - How many columns to return. Similar to LIMIT 100 in SQL.
                      May be arbitrarily large, but Thrift will materialize the whole result into memory before
                      returning it to the client, so be aware that you may be better served by iterating through
                      slices by passing the last value of one call in as the start of the next instead of increasing
                      count arbitrarily large."
  [^String start ^String finish & {:keys [count reversed]}]
  (let [slice-range (-> (SliceRange.)
                        (.setStart (to-byte-buffer start))
                        (.setFinish (to-byte-buffer finish)))]
    (when count
      (.setCount count))
    (when reversed
      (.setReversed count))
    slice-range))

(defn build-slice-predicate
  [range]
  (-> (SlicePredicate.)
      (.setSlice_range range)))

(defn build-column-parent
  [^String column-family]
  (ColumnParent. column-family))

(defn build-column-path
  [^String column-family ^String field type]
  (let [column-path (ColumnPath. column-family)]
    (if (= type :super)
      (.setSuper_column column-path (to-byte-buffer field))
      (.setColumn column-path (to-byte-buffer field)))
    column-path))

;;
;; Map conversions
;;

(defprotocol DefinitionToMap
  (to-map [input] "Converts any definition to map"))

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

  List
  (to-map [list]
    (map to-map list))

  nil
  (to-map [_]
    nil)

  Object
  (to-map [obj]
    obj))

(defprotocol ToPlainHash
  (to-plain-hash
    [input]
    [input key-format]
    ""))

(extend-protocol ToPlainHash
  java.util.List
  (to-plain-hash
    ([cosc-map]
       (to-plain-hash cosc-map "UTF8Type"))
    ([cosc-map key-format]
       (let [list  (map to-map cosc-map)
             names (reverse (map #(or
                                   (keyword (:name %))
                                   (cb/deserialize key-format (:key %)))
                                 list))
             values (reverse (map #(to-plain-hash (or (:columns %) (:value %))) list))]
         (zipmap names values))))

  ColumnOrSuperColumn
  (to-plain-hash
    ([cosc]
       (to-plain-hash cosc "UTF8Type"))
    ([cosc key-format]
       (to-plain-hash (list cosc) key-format)))

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
       nil))
  )