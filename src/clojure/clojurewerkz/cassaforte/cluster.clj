(ns clojurewerkz.cassaforte.cluster
  (:require [clojurewerkz.cassaforte.cluster.client :as cc]
            [clojurewerkz.cassaforte.bytes  :as cb]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.cql.query :as query]
            [clojurewerkz.cassaforte.utils :as utils]
            [qbits.hayt.cql :as cql])
  (:import [com.datastax.driver.core Session BoundStatement]))

(def ^:dynamic *debug-output* false)
(def ^:dynamic *default-consistency-level* org.apache.cassandra.db.ConsistencyLevel/ONE)

(defn ^clojure.lang.IPersistentMap execute-raw
  [^String query]
  (-> (.execute cc/*client* query)
))

(defn execute-prepared
  "Executes a CQL query previously prepared using the prepare-cql-query function
   by providing the actual values for placeholders"
  [[^String query ^java.util.List values]]
  (let [^BoundStatement bound-statement (.bind (.prepare cc/*client* query) (to-array values))]
    (-> (.execute cc/*client* bound-statement)
        ))
  )

(defn maybe-output-debug
  [q]
  (when *debug-output*
    (println "Built query: " q))
  q)

(defmacro prepared
  [& body]
  `(binding [cql/*prepared-statement* true
             cql/*param-stack* (atom [])]
     (do ~@body)))

(defn execute*
  [query-params builder]
  (utils/with-native-exception-handling ;; add check for throw exceptions
    (let [renderer (if cql/*prepared-statement* query/->prepared query/->raw)
          executor (if cql/*prepared-statement* execute-prepared execute-raw)]
      (-> (apply builder (flatten query-params))
          renderer
          maybe-output-debug
          executor))))


(defn drop-keyspace
  [ks]
  (execute* [ks] query/drop-keyspace-query))

(defn create-keyspace
  [& query-params]
  (execute* query-params query/create-keyspace-query))

(defn create-table
  [& query-params]
  (execute* query-params query/create-table-query))



(defn drop-table
  [ks]
  (execute* [ks] query/drop-table-query))

(defn use-keyspace
  [ks]
  (execute* [ks] query/use-keyspace-query))

(defn insert
  [& query-params]
  (execute* query-params query/insert-query))

(defn select
  [& query-params]
  (execute* query-params query/select-query))

(defn get-one
  [& query-params]
  (execute* query-params query/select-query))

(defn describe-keyspace
  [ks]
  (first
   (select :system.schema_keyspaces
           (query/where :keyspace_name ks))))

(defn describe-table
  [ks table]
  (first
   (select :system.schema_columnfamilies
           (query/where :keyspace_name ks
                        :columnfamily_name table))))

(defn describe-columns
  [ks table]
  (select :system.schema_columns
          (query/where :keyspace_name ks
                       :columnfamily_name table)))


(defn alter-table
  [& query-params]
  (execute* query-params query/alter-table-query))
