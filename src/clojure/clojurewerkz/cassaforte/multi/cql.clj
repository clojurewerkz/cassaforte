(ns clojurewerkz.cassaforte.multi.cql
  "Key CQL operations interface, prepared statement implementation, convenience functions
   for key operations built on top of CQL. Includes versions of cassaforte.cql functions that
   take database as an explicit argument.

   Use these namespace when you need to work with multiple databases or manage database
   and connection lifecycle explicitly."
  (:require
   [qbits.hayt.cql :as cql]
   [clojurewerkz.cassaforte.query :as query]
   [clojurewerkz.cassaforte.client :as client])
  (:import [com.datastax.driver.core Session]))

(defn ^:private execute-
  [^Session session query-params builder]
  (let [rendered-query (client/render-query (client/compile-query query-params builder))]
    (client/execute session rendered-query :prepared cql/*prepared-statement*)))

;;
;; Schema operations
;;

(defn drop-keyspace
  [^Session session ks]
  (execute- session [ks] query/drop-keyspace-query))

(defn create-keyspace
  [^Session session & query-params]
  (execute- session query-params query/create-keyspace-query))

(defn create-index
  [^Session session & query-params]
  (execute- session query-params query/create-index-query))

(defn drop-index
  [^Session session & query-params]
  (execute- session query-params query/drop-index-query))

(defn create-table
  [^Session session & query-params]
  (execute- session query-params query/create-table-query))

(def create-column-family create-table)

(defn drop-table
  [^Session session ks]
  (execute- session [ks] query/drop-table-query))

(defn use-keyspace
  [^Session session ks]
  (execute- session [ks] query/use-keyspace-query))

(defn alter-table
  [^Session session & query-params]
  (execute- session query-params query/alter-table-query))

(defn alter-keyspace
  [^Session session & query-params]
  (execute- session query-params query/alter-keyspace-query))

;;
;; DB Operations
;;

(defn insert
  [^Session session & query-params]
  (execute- session query-params query/insert-query))

(defn insert-batch
  [^Session session table records]
  (->> (map #(query/insert-query table %) records)
       (apply query/queries)
       query/batch-query
       client/render-query
       client/execute))

(defn update
  [^Session session & query-params]
  (execute- session query-params query/update-query))

(defn delete
  [^Session session & query-params]
  (execute- session query-params query/delete-query))

(defn select
  [^Session session & query-params]
  (execute- session query-params query/select-query))

(defn truncate
  [^Session session table]
  (execute- session [table] query/truncate-query))

;;
;; Higher level DB functions
;;

(defn perform-count
  [^Session session table & query-params]
  (:count
   (first
    (select session
            table
            (cons
             (query/columns (query/count*))
             query-params)))))

;;
;; Higher-level helper functions for schema
;;

(defn describe-keyspace
  [^Session session ks]
  (first
   (select session
           :system.schema_keyspaces
           (query/where :keyspace_name ks))))

(defn describe-table
  [^Session session ks table]
  (first
   (select session
           :system.schema_columnfamilies
           (query/where :keyspace_name ks
                        :columnfamily_name table))))

(defn describe-columns
  [^Session session ks table]
  (select session
          :system.schema_columns
          (query/where :keyspace_name ks
                       :columnfamily_name table)))

;;
;; Higher-level collection manipulation
;;

(defn- get-chunk
  "Returns next chunk for the lazy world iteration"
  [^Session session table partition-key chunk-size last-pk]
  (if (nil? last-pk)
    (select session
            table
            (query/limit chunk-size))
    (select session
            table
            (query/where (query/token partition-key) [> (query/token last-pk)])
            (query/limit chunk-size))))

(defn iterate-world
  "Lazily iterates through the collection, returning chunks of chunk-size."
  ([^Session session table partition-key chunk-size]
     (iterate-world table partition-key chunk-size []))
  ([^Session session table partition-key chunk-size c]
     (lazy-cat c
               (let [last-pk    (get (last c) partition-key)
                     next-chunk (get-chunk session table partition-key chunk-size last-pk)]
                 (iterate-world table partition-key chunk-size next-chunk)))))
