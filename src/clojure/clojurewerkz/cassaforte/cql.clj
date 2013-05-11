(ns clojurewerkz.cassaforte.cql
  (:import [com.datastax.driver.core Session])
  (:use clojurewerkz.cassaforte.cluster.conversion)
  (:require [clojurewerkz.cassaforte.query :as query]
            [qbits.hayt.cql :as cql]
            [clojurewerkz.cassaforte.cluster.client :as cluster]
            [clojurewerkz.cassaforte.debug-utils :as debug-utils]))

(def ^:dynamic *client*)
(def ^:dynamic *debug* false)

(defmacro with-client
  [^Session client & body]
  `(binding [*client* ~client]
     (do ~@body)))

(defmacro with-debug
  "Executes query with debug output"
  [& body]
  `(binding [*debug* true]
     (debug-utils/catch-exceptions ~@body)))

(defn connect
  "Connects to the C* cluster, returns Session"
  [h]
  (cond
   (vector? h) (cluster/connect h)))

(defn connect!
  "Connects to C* cluster, sets *client* as a default client"
  [h]
  (let [c (connect h)]
    (alter-var-root (var *client*) (constantly c))
    c))

;; Ability to turn on and off prepared statements by default? Turn on prepared statements on per-query basis
;; (macro+binding)

(defmacro prepared
  "Helper macro to execute prepared statement"
  [& body]
  `(binding [cql/*prepared-statement* true
             cql/*param-stack*        (atom [])]
     (do ~@body)))

(defn execute
  [query-params builder]
  (let [executor (if cql/*prepared-statement* cluster/execute-prepared cluster/execute)
        renderer (if cql/*prepared-statement* query/->prepared query/->raw)
        query    (->> query-params
                      flatten
                      (apply builder)
                      renderer)]
    (when *debug*
      (debug-utils/output-debug query))
    (executor *client* query)))

;;
;; Schema operations
;;

(defn drop-keyspace
  [ks]
  (execute [ks] query/drop-keyspace-query))

(defn create-keyspace
  [& query-params]
  (execute query-params query/create-keyspace-query))

(defn create-table
  [& query-params]
  (execute query-params query/create-table-query))

(def create-column-family create-table)

(defn drop-table
  [ks]
  (execute [ks] query/drop-table-query))

(defn use-keyspace
  [ks]
  (execute [ks] query/use-keyspace-query))

(defn alter-table
  [& query-params]
  (execute query-params query/alter-table-query))

(defn alter-keyspace
  [& query-params]
  (execute query-params query/alter-keyspace-query))

;;
;; DB Operations
;;

(defn insert
  [& query-params]
  (execute
   query-params
   query/insert-query))

(defn update
  [& query-params]
  (execute
   query-params
   query/update-query))

(defn delete
  [& query-params]
  (execute
   query-params
   query/delete-query))

(defn select
  [& query-params]
  (execute query-params query/select-query))

(defn truncate
  [table]
  (execute [table] query/truncate-query))

;;
;; Higher level DB functions
;;

;; TBD, add Limit
(defn get-one
  [& query-params]
  (execute query-params query/select-query))

(defn perform-count
  [table & query-params]
  (:count
   (first
    (select table
            (cons
             (query/columns (query/count*))
             query-params)))))

;;
;; Higher-level helper functions for schema
;;

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
