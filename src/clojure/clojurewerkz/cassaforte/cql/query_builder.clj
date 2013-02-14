(ns clojurewerkz.cassaforte.cql.query-builder
  (:use [clojure.string :only [split join trim trimr escape upper-case]]
        [clojurewerkz.support.string :only [maybe-append interpolate-vals interpolate-kv]]))

(defprotocol CQLValue
  (to-cql-value
    [value]
    [value opts]"Converts the given value to a CQL string representation"))

(def ^:dynamic *cql-stack* nil)

(defn- to-cql-value-wrapper
  [f v]
  (if *cql-stack*
    (do
      (conj! *cql-stack* v)
      "?")
    (f)))

(extend-protocol CQLValue
  nil
  (to-cql-value [value]
    (to-cql-value-wrapper
     (fn [] "null")
     value))

  Number
  (to-cql-value [^Number value]
    (to-cql-value-wrapper
     #(str value)
     value))

  Long
  (to-cql-value [^Long value]
    (to-cql-value-wrapper
     #(str value)
     value))

  clojure.lang.BigInt
  (to-cql-value [^clojure.lang.BigInt value]
    (to-cql-value-wrapper
     #(str value)
     value))

  clojure.lang.Named
  (to-cql-value [^clojure.lang.Named value]
    (to-cql-value-wrapper
     #(name value)
     value))

  java.util.Date
  (to-cql-value [^java.util.Date value]
    (to-cql-value-wrapper
     #(to-cql-value (.getTime value))
     value))

  String
  (to-cql-value [^String value]
    (to-cql-value-wrapper
     #(str "'" (escape value {\" "\""}) "'")
     value))

  clojure.lang.LazySeq
  (to-cql-value
    ([^clojure.lang.LazySeq value]
       (to-cql-value value nil))
    ([^clojure.lang.LazySeq value opts]
       (to-cql-value-wrapper
        #(to-cql-value (vec value) opts)
        value)))

  clojure.lang.PersistentVector
  (to-cql-value
    ([^clojure.lang.PersistentVector value {:keys [skip-brackets]}]
       (to-cql-value-wrapper
        #(let [v (join ", " (map to-cql-value value))]
          (if skip-brackets
            v
            (str "[" v "]")))
        value))
    ([^clojure.lang.PersistentVector value]
       (to-cql-value value {:skip-brackets false})))

  clojure.lang.IPersistentMap
  (to-cql-value [^clojure.lang.PersistentVector value]
    (to-cql-value-wrapper
     #(str "{"
       (join
         ", "
         (map
          (fn [[k v]] (str (to-cql-value k) ":" (to-cql-value v)))
          value))
       "}")
     value))

  )

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
                                         (interpolate-kv primary-key-clause
                                                         {:column (to-cql-value
                                                                   (flatten [primary-key]) {:skip-brackets true})}))}))

(defn prepare-drop-column-family-query
  [column-family]
  (interpolate-kv drop-column-family-query {:column-family-name column-family}))


(def insert-query
  "INSERT INTO :column-family-name (:names) VALUES (:values):opts;")

(def update-query
  "UPDATE :column-family-name SET :kvps :where-clause")

(def opts-clause
  " USING ?")

(def index-name-clause
  " :index-name")

(def create-index-query
  "CREATE INDEX:index-name ON :column-family-name (:column-name)")

(defn prepare-create-index-query
  [column-family column-name]
  (interpolate-kv create-index-query
                  {:index-name (interpolate-kv index-name-clause {:index-name (str column-family "_" column-name "_idx")})
                   :column-family-name column-family
                   :column-name (name column-name)}))

(def select-query
  "SELECT :columns-clause FROM :column-family-name :where-clause:order-clause:limit-clause")

(def ^{:const true} in "IN")

(defn match-operation
  [[operation orig-value & rest]]
  (let [res (cond
             (= operation >) (format " > %s" (to-cql-value orig-value))
             (= operation >=) (format " >= %s" (to-cql-value orig-value))
             (= operation <) (format " < %s" (to-cql-value orig-value))
             (= operation <=) (format " <= %s" (to-cql-value orig-value))
             (= operation =) (format " = %s" (to-cql-value orig-value))
             (= operation :in) (format " IN (%s)" (to-cql-value orig-value {:skip-brackets true}))
             (keyword? operation) (format "%s %s" (name operation) (to-cql-value orig-value)))]
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


(defn prepare-update-query
  [column-family m & {:keys [timestamp where ttl as-prepared-statement] :as opts}]
  (if as-prepared-statement
    (binding [*cql-stack* (transient [])]
      (let [res (apply prepare-update-query column-family m (apply concat (dissoc opts :as-prepared-statement)))]
        [(persistent! *cql-stack*) res]))
    (interpolate-kv update-query
                    {:column-family-name column-family
                     :kvps (join ", " (map (fn [[k v]] (str (name k) " = " (to-cql-value v))) m))
                     :where-clause (when where
                                     (prepare-where-clause where))})))

(defn prepare-insert-query
  [column-family m & {:keys [timestamp ttl as-prepared-statement] :as opts}]
  (if as-prepared-statement
    (binding [*cql-stack* (transient [])]
      (let [res (apply prepare-insert-query column-family m (apply concat (dissoc opts :as-prepared-statement)))]
        [(persistent! *cql-stack*) res]))
    (interpolate-kv insert-query
                    {:column-family-name column-family
                     :names (trim (join ", " (map #(name %) (keys m))))
                     :values (trim (join ", " (map #(to-cql-value %) (vals m))))
                     :opts (when opts
                             (interpolate-vals
                              opts-clause
                              [(join " AND " (map (fn [[k v]] (str (upper-case (name k)) " " v)) opts))]))})))

(defn prepare-select-query
  [column-family & {:keys [columns where limit order as-prepared-statement] :as opts}]
  (if as-prepared-statement
    (binding [*cql-stack* (transient [])]
      (let [res (apply prepare-select-query column-family (apply concat (dissoc opts :as-prepared-statement)))]
        [(persistent! *cql-stack*) res]))
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
                      }))))
