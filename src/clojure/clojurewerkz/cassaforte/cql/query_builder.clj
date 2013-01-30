(ns clojurewerkz.cassaforte.cql.query-builder
  (:use [clojure.string :only [split join trim trimr escape upper-case]]
        [clojurewerkz.support.string :only [maybe-append interpolate-vals interpolate-kv]]))

(defprotocol CQLValue
  (to-cql-value [value] "Converts the given value to a CQL string representation"))

(extend-protocol CQLValue
  nil
  (to-cql-value [value]
    "null")

  Number
  (to-cql-value [^Number value]
    (str value))

  Long
  (to-cql-value [^Long value]
    (str value))

  clojure.lang.BigInt
  (to-cql-value [^clojure.lang.BigInt value]
    (str value))

  clojure.lang.Named
  (to-cql-value [^clojure.lang.Named value]
    (name value))

  java.util.Date
  (to-cql-value [^java.util.Date value]
    (to-cql-value (.getTime value)))

  String
  (to-cql-value [^String value]
    (str "'" (escape value {\" "\""}) "'"))

  clojure.lang.PersistentVector
  (to-cql-value [^clojure.lang.PersistentVector value]
    (join ", " (map to-cql-value value))))

(def primary-key-clause
  ", PRIMARY KEY (:column)")

(def create-query
  "CREATE TABLE :column-family-name (:column-definitions:primary-key-clause);")

(def drop-column-family-query
  "DROP TABLE :column-family-name;")

(defn prepare-create-column-family-query
  [column-family column-definitions & {:keys [primary-key]}]
  (interpolate-kv create-query
                  {:column-family-name column-family
                   :column-definitions (trim (join ", " (map (fn [[k v]] (str (name k) " " (name v))) column-definitions) ))
                   :primary-key-clause (when primary-key
                                         (interpolate-kv primary-key-clause {:column (to-cql-value primary-key)}))}))

(defn prepare-drop-column-family-query
  [column-family]
  (interpolate-kv drop-column-family-query {:column-family-name column-family}))


(def insert-query
  "INSERT INTO :column-family-name (:names) VALUES (:values):opts;")

(def opts-clause
  " USING ?")

(defn prepare-insert-query
  [column-family m & {:keys [timestamp ttl] :as opts}]
  (interpolate-kv insert-query
                  {:column-family-name column-family
                   :names (trim (join ", " (map #(name %) (keys m))))
                   :values (trim (join ", " (map #(to-cql-value %) (vals m))))
                   :opts (when opts
                           (interpolate-vals
                            opts-clause
                            [(join " AND " (map (fn [[k v]] (str (upper-case (name k)) " " v)) opts))]))}))

(def index-name-clause
  " :index-name")

(def create-index-query
  "CREATE INDEX:index-name ON :column-family-name (:column-name)")

(defn prepare-create-index-query
  ([column-family column-name]
     (prepare-create-index-query column-family column-name nil))
  ([column-family column-name index-name]
     (interpolate-kv create-index-query
                     {:index-name (when index-name
                                    (interpolate-kv index-name-clause {:index-name index-name}))
                      :column-family-name column-family
                      :column-name (name column-name)})))

(def select-query
  "SELECT :columns-clause FROM :column-family-name :where-clause:order-clause:limit-clause")

(def ^{:const true} in "IN")

(defn match-operation
  [[operation orig-value & rest]]
  (let [value (to-cql-value orig-value)
        res   (cond
               (= operation >) (format " > %s" value)
               (= operation >=) (format " >= %s" value)
               (= operation <) (format " < %s" value)
               (= operation <=) (format " <= %s" value)
               (= operation =) (format " = %s" value)
               (= operation :in) (format " IN (%s)" value)
               (keyword? operation) (format "%s %s" (name operation) value))]
    (if rest
      (conj [res] (match-operation rest))
      [res])))

(defn operation-clause
  [[orig-key value]]
  (let [key (name orig-key)]
    (cond
      (vector? value) (for [item (flatten (match-operation value))]
                        (str key item))
      :else (format "%s = %s" key (to-cql-value value)))))

(defn prepare-where-clause
  [kvs]
  (str "WHERE "
       (join " AND " (flatten (map
                               operation-clause
                               kvs)))))

(defn prepare-limit-clause
  [limit]
  (str " LIMIT " limit))

(defn prepare-order-clause
  [limit]
  (str " ORDER BY "
       (if (vector? limit)
         (let [[order direction & rest] limit]
           (when (not (empty? rest))
             (throw (Exception. "Limit clause should contain exactly 2 iterms, column and direction")))
           (str (to-cql-value order) " " (if direction (upper-case (to-cql-value direction)) "")))
         (to-cql-value limit))))

(defn prepare-select-query
  [column-family & {:keys [columns where limit order]}]
  (trim
   (interpolate-kv select-query
                   {:column-family-name column-family
                    :columns-clause (if columns
                                      (join ", " columns)
                                      "*")
                    :where-clause (when where
                                    (prepare-where-clause where))
                    :order-clause (when order
                                    (trimr (prepare-order-clause order)))
                    :limit-clause (when limit
                                    (prepare-limit-clause limit))
                    })))
